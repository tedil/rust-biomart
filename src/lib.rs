use csv::StringRecord;
use itertools::Itertools;
use maplit::hashmap;
use serde::export::fmt::Debug;
use serde::{Deserialize, Deserializer};
use serde_derive::Deserialize;
use serde_xml_rs::from_reader;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use xmltree::{Element, XMLNode};

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

    pub fn query(&self, query: &Query) -> Result<Response, Box<dyn Error>> {
        let url = &self.server;
        let s = query.to_string();

        let response = self.client.post(url).query(&[("query", s)]).send().unwrap();
        if response.status().is_success() {
            Ok(Response {
                raw: response.text()?,
            })
        } else if response.status().is_server_error() {
            Err(Box::new(ServerError))
        } else {
            Err(Box::new(StatusError(response.status())))
        }
    }

    pub fn marts(&self) -> Result<Vec<MartURLLocation>, Box<dyn Error>> {
        let response = self
            .client
            .post(&self.server)
            .query(&[("type", "registry")])
            .send()
            .unwrap();
        if response.status().is_success() {
            let xml = response.text()?;
            let registry: MartRegistry = from_reader(xml.as_bytes())?;
            Ok(registry.marts)
        } else {
            Err(Box::new(StatusError(response.status())))
        }
    }
}

fn default_on_error_deserializer<'de, D, T>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + FromStr + Deserialize<'de>,
    <T as std::str::FromStr>::Err: Debug,
{
    let v = T::deserialize(d);
    match v {
        Ok(v) => Ok(v),
        _ => Ok(T::default()),
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct MartRegistry {
    #[serde(rename = "MartURLLocation", default)]
    pub marts: Vec<MartURLLocation>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MartURLLocation {
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

pub struct QueryBuilder {
    mart: String,
    dataset: String,
    filters: Vec<(String, Vec<String>)>,
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

    pub fn filter<S: Into<String>, I: IntoIterator<Item = S>>(
        &mut self,
        filter: S,
        values: I,
    ) -> &mut Self {
        self.filters.push((
            filter.into(),
            values.into_iter().map(|s| s.into()).collect(),
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
            let values: String = values.iter().join(",");
            v.children.push(XMLNode::Element(Element {
                prefix: None,
                namespace: None,
                namespaces: None,
                name: "Filter".into(),
                attributes: hashmap! {"name".into() => filter.to_string(), "value".into() => values},
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
    use crate::{MartClient, MartRegistry, MartURLLocation, QueryBuilder};
    use serde_xml_rs::from_reader;

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
    fn parse_marts() {
        let data = r##"<MartRegistry>
    <MartURLLocation database="ensembl_mart_99" default="1" displayName="Ensembl Genes 99" host="www.ensembl.org" includeDatasets="" martUser="" name="ENSEMBL_MART_ENSEMBL" path="/biomart/martservice" port="80" serverVirtualSchema="default" visible="1" />
</MartRegistry>"##;
        let registry: MartRegistry = from_reader(data.as_bytes()).unwrap();
        let expected = MartRegistry {
            marts: vec![MartURLLocation {
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
