use std::error::Error;
use std::fmt::{Display, Formatter};

use csv::StringRecord;
use itertools::Itertools;
use maplit::hashmap;
use serde::de::Unexpected;
use serde::export::fmt::Debug;
use serde::{de, Deserialize, Deserializer};
use serde_xml_rs::from_reader;
use xmltree::{Element, XMLNode};

use serde_with;
use serde_with::CommaSeparator;

mod definitions;

pub struct MartClient {
    server: String,
    client: reqwest::blocking::Client,
}

impl MartClient {
    pub fn new(server: String) -> Self {
        MartClient {
            server,
            client: reqwest::blocking::Client::new(),
        }
    }

    fn make_request(&self, query: &[(&str, &str)]) -> Result<String, Box<dyn Error>> {
        dbg!(&self.client.post(&self.server).query(query));
        let response = self.client.post(&self.server).query(query).send()?;
        if response.status().is_success() {
            let xml = response.text()?;
            Ok(xml)
        } else {
            Err(Box::new(StatusError(response.status())))
        }
    }

    fn request_and_parse<P, R>(
        &self,
        query: &[(&str, &str)],
        parser: P,
    ) -> Result<R, Box<dyn Error>>
    where
        P: FnOnce(String) -> Result<R, Box<dyn Error>>,
    {
        self.make_request(query).and_then(parser)
    }

    pub fn query(&self, query: &Query) -> Result<Response, Box<dyn Error>> {
        let s = query.to_string();
        self.make_request(&[("query", &s)])
            .map(|xml| Response { raw: xml })
    }

    pub fn marts(&self) -> Result<Vec<MartInfo>, Box<dyn Error>> {
        self.request_and_parse(&[("type", "registry")], |xml| {
            let registry: MartRegistry = from_reader(xml.as_bytes())
                .unwrap_or_else(|_| panic!("Failed parsing xml: {:?}", &xml));
            Ok(registry.marts)
        })
    }

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
}

#[derive(Debug)]
pub struct Response {
    raw: String,
}

impl Response {
    pub fn data(&self) -> Vec<StringRecord> {
        csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .from_reader(self.raw.as_bytes())
            .records()
            .filter_map(Result::ok)
            .collect()
    }
}

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
pub struct FilterInfo {
    name: String,
    description: String,
    #[serde(with = "serde_with::rust::StringWithSeparator::<CommaSeparator>")]
    options: Vec<String>,
    full_description: String,
    filters: String,
    kind: FilterType,
    operation: String, // "=", ">=", "<=", "in", "only", "excluded", "=,in", "only,excluded", …
    unknown_1: String,
    unknown_2: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct MartRegistry {
    #[serde(rename = "MartURLLocation", default)]
    pub marts: Vec<MartInfo>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
#[serde(rename = "MartURLLocation")]
pub struct MartInfo {
    pub host: String,
    pub port: usize,
    pub database: String,
    // TODO include_datasets should be a collection, not a String
    #[serde(default, deserialize_with = "default_on_error_deserializer")]
    pub include_datasets: String,
    #[serde(default, deserialize_with = "default_on_error_deserializer")]
    pub visible: bool,
    #[serde(default, deserialize_with = "default_on_error_deserializer")]
    pub mart_user: String,
    #[serde(default, deserialize_with = "default_on_error_deserializer")]
    pub default: bool,
    pub server_virtual_schema: String,
    pub display_name: String,
    pub path: String,
    pub name: String,
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
        let data = r##"
        <?xml version='1.0' encoding='UTF-8'?><!DOCTYPE Query>
            <Query
                virtualSchemaName='default'
                uniqueRows='1'
                count='0'
                datasetConfigVersion='0.6'
                header='1'
                formatter='TSV'
                requestid='rust-biomart'
            >
                <Dataset name = ''>
                </Dataset>
            </Query>"##;
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

fn default_on_error_deserializer<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    let v = T::deserialize(d);
    match v {
        Ok(v) => Ok(v),
        _ => Ok(T::default()),
    }
}

fn bool_from_int<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(de::Error::invalid_value(
            Unexpected::Unsigned(other as u64),
            &"zero or one",
        )),
    }
}

#[derive(Debug)]
struct ServerError;

#[derive(Debug)]
struct StatusError(reqwest::StatusCode);

impl Error for ServerError {}

impl Error for StatusError {}

impl Display for ServerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str("Server error")
    }
}

impl Display for StatusError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_fmt(format_args!("Error, status code: {}", self.0))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use serde_xml_rs::from_reader;

    use crate::{MartClient, MartInfo, MartRegistry, QueryBuilder};

    #[test]
    fn it_works() {
        let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice".into());
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
        let response = mart_client.query(&query);
        assert_eq!(
            "AFFY HG U133 Plus 2 probe	NCBI gene ID
209310_s_at	837
207500_at	838
202763_at	836
",
            response.unwrap().raw
        );
    }

    #[test]
    fn list_datasets() {
        let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice".into());
        let datasets = mart_client.datasets("ENSEMBL_MART_ENSEMBL");
        dbg!(datasets);
    }

    #[test]
    fn list_filters() {
        let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice".into());
        let filters = mart_client.filters("ENSEMBL_MART_ENSEMBL", "hsapiens_gene_ensembl");
        dbg!(filters.map(|f| f.iter().take(1).cloned().collect_vec()));
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
                include_datasets: "".to_string(),
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
