use std::error::Error;

use csv::StringRecord;
use getset::{Getters, MutGetters, Setters};
use itertools::Itertools;
use maplit::hashmap;
use reqwest::blocking::Client;
use serde::export::fmt::Debug;
use serde::{Deserialize, Serialize};
use serde_with;
use serde_with::CommaSeparator;
use serde_xml_rs::from_reader;
use xmltree::{Element, XMLNode};

use crate::definitions::{bool_from_int, default_on_error_deserializer, StatusError};
use std::time::Duration;

mod definitions;

const REQUEST_ID: &str = "rust-biomart";

pub struct MartClient {
    server: String,
    client: Client,
}

impl MartClient {
    pub fn new<S: Into<String>>(server: S) -> Self {
        MartClient {
            server: server.into(),
            client: Client::builder()
                .timeout(Duration::from_secs(60))
                .gzip(true)
                .build()
                .unwrap_or_else(|_| Client::new()),
        }
    }

    fn make_request<T: Serialize + ?Sized>(&self, query: &T) -> Result<String, Box<dyn Error>> {
        let q = self
            .client
            .post(&self.server)
            .header(reqwest::header::ACCEPT_ENCODING, "gzip")
            .query(&[("requestid", REQUEST_ID)])
            .query(query);
        let response = q.send()?;
        if response.status().is_success() {
            let text = response.text()?;
            Ok(text)
        } else {
            Err(Box::new(StatusError(response.status())))
        }
    }

    fn request_and_parse<P, R, T>(&self, query: &T, parser: P) -> Result<R, Box<dyn Error>>
    where
        P: FnOnce(String) -> Result<R, Box<dyn Error>>,
        T: Serialize + ?Sized,
    {
        self.make_request(query).and_then(parser)
    }

    pub fn query(&self, query: &Query) -> Result<Response, Box<dyn Error>> {
        let s = query.to_string();
        self.request_and_parse(&[("query", &s)], |xml| Ok(Response { raw: xml }))
    }

    /// Lists available marts for given registry.
    ///
    /// # Example
    /// ```
    /// use rust_biomart::MartClient;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
    /// let marts = mart_client.marts()?;
    /// for mart in &marts {
    ///     println!("{}", mart.name());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn marts(&self) -> Result<Vec<MartInfo>, Box<dyn Error>> {
        self.request_and_parse(&[("type", "registry")], |xml| {
            let registry: MartRegistry = from_reader(xml.as_bytes())
                .unwrap_or_else(|_| panic!("Failed parsing xml: {:?}", &xml));
            Ok(registry.marts)
        })
    }

    /// Lists available datasets for a given mart.
    ///
    /// # Arguments
    ///
    /// * `mart` - Name of the Mart for which available datasets are to be queried. See also `MartClient::name`.
    ///
    /// # Example
    ///
    /// ```
    /// use rust_biomart::MartClient;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
    /// let datasets = mart_client.datasets("ENSEMBL_MART_ENSEMBL")?;
    /// for info in &datasets {
    ///     println!("{}: {}", info.dataset(), info.description());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn datasets(&self, mart: &str) -> Result<Vec<DatasetInfo>, Box<dyn Error>> {
        self.request_and_parse(&[("mart", mart), ("type", "datasets")], |xml| {
            Ok(csv::ReaderBuilder::new()
                .has_headers(false)
                .delimiter(b'\t')
                .from_reader(xml.trim().as_bytes())
                .deserialize::<DatasetInfo>()
                .filter_map(Result::ok)
                .collect())
        })
    }

    /// Lists available filters for a given mart+dataset.
    ///
    /// # Arguments
    ///
    /// * `mart` - Name of the Mart. See also `MartClient::name`.
    /// * `dataset` - Name of the dataset. See also `DatasetInfo::dataset`.
    ///
    /// # Example
    ///
    /// ```
    /// use rust_biomart::MartClient;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
    /// let filters = mart_client.filters("ENSEMBL_MART_ENSEMBL", "hsapiens_gene_ensembl")?;
    /// for filter in &filters {
    ///     println!("{}: {}", filter.name(), filter.description());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn filters(&self, mart: &str, dataset: &str) -> Result<Vec<FilterInfo>, Box<dyn Error>> {
        self.request_and_parse(
            &[("mart", mart), ("dataset", dataset), ("type", "filters")],
            |tsv| {
                Ok(csv::ReaderBuilder::new()
                    .has_headers(false)
                    .delimiter(b'\t')
                    .from_reader(tsv.trim().as_bytes())
                    .deserialize::<FilterInfo>()
                    .filter_map(Result::ok)
                    // FIXME: write deserializer that can handle Vec<String> representations like "[v_1, v_2, …, v_n]"
                    .map(|mut info| match info.options.len() {
                        0 => info,
                        1 => {
                            let s: String = info.options[0]
                                .trim_matches(|c| c == '[' || c == ']')
                                .into();
                            if !s.is_empty() {
                                info.options[0] = s;
                            } else {
                                info.options.clear();
                            }
                            info
                        }
                        _ => {
                            let n = info.options.len() - 1;
                            info.options[0] = info.options[0].trim_matches('[').into();
                            info.options[n] = info.options[n].trim_matches(']').into();
                            info
                        }
                    })
                    .collect())
            },
        )
    }

    /// Lists available attributes for a given mart+dataset.
    ///
    /// # Arguments
    ///
    /// * `mart` - Name of the Mart. See also `MartClient::name`.
    /// * `dataset` - Name of the dataset. See also `DatasetInfo::dataset`.
    ///
    /// # Example
    ///
    /// ```
    /// use rust_biomart::MartClient;
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
    /// let attributes = mart_client.attributes("ENSEMBL_MART_ENSEMBL", "hsapiens_gene_ensembl")?;
    /// for attribute in &attributes {
    ///     println!("{}: {}", attribute.name(), attribute.description());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn attributes(
        &self,
        mart: &str,
        dataset: &str,
    ) -> Result<Vec<AttributeInfo>, Box<dyn Error>> {
        self.request_and_parse(
            &[("mart", mart), ("dataset", dataset), ("type", "attributes")],
            |tsv| {
                Ok(csv::ReaderBuilder::new()
                    .has_headers(false)
                    .delimiter(b'\t')
                    .from_reader(tsv.trim().as_bytes())
                    .deserialize::<AttributeInfo>()
                    .filter_map(Result::ok)
                    .collect())
            },
        )
    }
}

#[derive(Debug)]
pub struct Response {
    raw: String,
}

impl Response {
    pub fn raw(&self) -> &str {
        &self.raw
    }

    pub fn header(&self) -> Option<StringRecord> {
        csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(self.raw.as_bytes())
            .headers()
            .ok()
            .cloned()
    }

    pub fn records(&self) -> Vec<StringRecord> {
        csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(self.raw.as_bytes())
            .records()
            .filter_map(Result::ok)
            .collect()
    }
}

#[derive(Debug, Deserialize, Getters, Setters, MutGetters)]
#[getset(set = "pub", get = "pub", get_mut = "pub")]
pub struct DatasetInfo {
    kind: String,
    dataset: String,
    description: String,
    #[serde(deserialize_with = "bool_from_int")]
    visible: bool,
    version: String,
    unknown_1: usize,
    unknown_2: usize,
    unknown_3: String,
    date: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterType {
    Boolean,
    BooleanList,
    IdList,
    List,
    Text,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, Deserialize, Getters, Setters, MutGetters)]
#[getset(set = "pub", get = "pub", get_mut = "pub")]
pub struct FilterInfo {
    name: String,
    description: String,
    #[serde(with = "serde_with::rust::StringWithSeparator::<CommaSeparator>")]
    options: Vec<String>,
    full_description: String,
    filters: String,
    kind: FilterType,
    // "=", ">=", "<=", "in", "only", "excluded", "=,in", "only,excluded", …
    operation: String,
    unknown_1: String,
    unknown_2: String,
}

#[derive(Debug, Clone, Deserialize, Getters, Setters, MutGetters)]
#[getset(set = "pub", get = "pub", get_mut = "pub")]
pub struct AttributeInfo {
    name: String,
    description: String,
    full_description: String,
    page: String,
    #[serde(with = "serde_with::rust::StringWithSeparator::<CommaSeparator>")]
    formats: Vec<String>,
    unknown_1: String,
    unknown_2: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct MartRegistry {
    #[serde(rename = "MartURLLocation", default)]
    pub marts: Vec<MartInfo>,
}

#[derive(Debug, Deserialize, PartialEq, Getters, Setters, MutGetters)]
#[serde(rename_all = "camelCase")]
#[serde(rename = "MartURLLocation")]
#[getset(set = "pub", get = "pub", get_mut = "pub")]
pub struct MartInfo {
    host: String,
    port: usize,
    database: String,
    #[serde(
        default,
        with = "serde_with::rust::StringWithSeparator::<CommaSeparator>"
    )]
    include_datasets: Vec<String>,
    #[serde(default, deserialize_with = "default_on_error_deserializer")]
    visible: bool,
    #[serde(default, deserialize_with = "default_on_error_deserializer")]
    mart_user: String,
    #[serde(default, deserialize_with = "default_on_error_deserializer")]
    default: bool,
    server_virtual_schema: String,
    display_name: String,
    path: String,
    name: String,
}

enum FilterOperation {
    Match(Vec<String>),
    Include,
    Exclude,
}

pub struct QueryBuilder {
    mart: String,
    dataset: String,
    filters: Vec<(String, FilterOperation)>,
    attributes: Vec<String>,
}

#[derive(Debug)]
pub struct Query {
    inner: Element,
}

impl ToString for Query {
    fn to_string(&self) -> String {
        let mut q = Vec::new();
        self.inner.write(&mut q).unwrap();
        String::from_utf8_lossy(&q).into()
    }
}

impl Default for Query {
    fn default() -> Self {
        let data = format!(
            r##"
        <?xml version='1.0' encoding='UTF-8'?><!DOCTYPE Query>
            <Query
                virtualSchemaName='default'
                uniqueRows='1'
                count='0'
                datasetConfigVersion='0.6'
                header='1'
                formatter='TSV'
                requestid='{requestid}'
            >
                <Dataset name = ''>
                </Dataset>
            </Query>"##,
            requestid = REQUEST_ID
        );
        let inner = Element::parse(data.as_bytes()).unwrap();
        Query { inner }
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        QueryBuilder {
            mart: "".into(),
            dataset: "".into(),
            filters: vec![],
            attributes: vec![],
        }
    }
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mart<S: Into<String>>(&mut self, mart: S) -> &mut Self {
        self.mart = mart.into();
        self
    }

    pub fn dataset<S: Into<String>>(&mut self, dataset: S) -> &mut Self {
        self.dataset = dataset.into();
        self
    }

    pub fn filter<T, S, I>(&mut self, filter: S, values: I) -> &mut Self
    where
        T: Into<String>,
        S: Into<String>,
        I: IntoIterator<Item = T>,
    {
        self.filters.push((
            filter.into(),
            FilterOperation::Match(values.into_iter().map(|s| s.into()).collect()),
        ));
        self
    }

    pub fn filter_bool<S: Into<String>>(&mut self, filter: S, include: bool) -> &mut Self {
        self.filters.push((
            filter.into(),
            if include {
                FilterOperation::Include
            } else {
                FilterOperation::Exclude
            },
        ));
        self
    }

    pub fn attribute<S: Into<String>>(&mut self, attribute: S) -> &mut Self {
        self.attributes.push(attribute.into());
        self
    }

    pub fn attributes<S: Into<String>, I: IntoIterator<Item = S>>(
        &mut self,
        attributes: I,
    ) -> &mut Self {
        for attribute in attributes {
            self.attribute(attribute);
        }
        self
    }

    pub fn build(&self) -> Query {
        let mut query = Query::default();

        query
            .inner
            .get_mut_child("Dataset")
            .expect("dataset")
            .attributes
            .insert("name".into(), (&self.dataset).to_owned());

        for (filter, values) in &self.filters {
            let v = query.inner.get_mut_child("Dataset").expect("dataset");
            let attributes = match values {
                FilterOperation::Match(values) => {
                    let s: String = values.iter().join(",");
                    hashmap! {"name".into() => filter.to_string(), "value".into() => s}
                }
                FilterOperation::Exclude => {
                    hashmap! {"name".into() => filter.to_string(), "excluded".into() => "1".into()}
                }
                FilterOperation::Include => {
                    hashmap! {"name".into() => filter.to_string(), "excluded".into() => "0".into()}
                }
            };

            v.children.push(XMLNode::Element(Element {
                prefix: None,
                namespace: None,
                namespaces: None,
                name: "Filter".into(),
                attributes,
                children: vec![],
            }))
        }
        for attribute in &self.attributes {
            let v = query.inner.get_mut_child("Dataset").expect("dataset");
            v.children.push(XMLNode::Element(Element {
                prefix: None,
                namespace: None,
                namespaces: None,
                name: "Attribute".into(),
                attributes: hashmap! {"name".into() => attribute.to_string()},
                children: vec![],
            }))
        }
        query
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use serde_xml_rs::from_reader;

    use crate::{MartClient, MartInfo, MartRegistry, QueryBuilder};

    #[test]
    fn it_works() {
        let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
        let query = QueryBuilder::new()
            .mart("ensembl")
            .dataset("hsapiens_gene_ensembl")
            .attributes(vec!["affy_hg_u133_plus_2", "entrezgene_id"])
            .filter(
                "affy_hg_u133_plus_2",
                vec!["202763_at", "209310_s_at", "207500_at"],
            )
            .build();
        println!("{}", &query.to_string());
        let response = mart_client.query(&query).unwrap();
        assert_eq!(
            "AFFY HG U133 Plus 2 probe	NCBI gene ID
209310_s_at	837
207500_at	838
202763_at	836
",
            response.raw()
        );
    }

    #[test]
    fn list_datasets() {
        let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
        let datasets = mart_client.datasets("ENSEMBL_MART_ENSEMBL");
        dbg!(datasets);
    }

    #[test]
    fn list_filters() {
        let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
        let filters = mart_client.filters("ENSEMBL_MART_ENSEMBL", "hsapiens_gene_ensembl");
        dbg!(filters.map(|f| f.iter().take(1).cloned().collect_vec()));
    }

    #[test]
    fn list_attributes() {
        let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
        let attributes = mart_client.attributes("ENSEMBL_MART_ENSEMBL", "hsapiens_gene_ensembl");
        dbg!(attributes.map(|f| f.iter().take(1).cloned().collect_vec()));
    }

    #[test]
    fn parse_marts() {
        let data = r##"<MartRegistry>
    <MartURLLocation database="ensembl_mart_99" default="1" displayName="Ensembl Genes 99" host="www.ensembl.org" includeDatasets="" martUser="" name="ENSEMBL_MART_ENSEMBL" path="/biomart/martservice" port="80" serverVirtualSchema="default" visible="1" />
</MartRegistry>"##;
        let registry: MartRegistry = from_reader(data.as_bytes()).unwrap();
        let expected = MartRegistry {
            marts: vec![MartInfo {
                host: "www.ensembl.org".to_string(),
                port: 80,
                database: "ensembl_mart_99".to_string(),
                include_datasets: vec![],
                visible: true,
                mart_user: "".to_string(),
                default: true,
                server_virtual_schema: "default".to_string(),
                display_name: "Ensembl Genes 99".to_string(),
                path: "/biomart/martservice".to_string(),
                name: "ENSEMBL_MART_ENSEMBL".to_string(),
            }],
        };
        assert_eq!(expected, registry);
    }
}
