use csv::StringRecord;
use itertools::Itertools;
use maplit::hashmap;
use std::error::Error;
use std::fmt::{Display, Formatter};
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
    use crate::{MartClient, QueryBuilder};

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
}
