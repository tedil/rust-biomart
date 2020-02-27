use serde::de::Unexpected;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(rename = "Query")]
pub struct XmlQuery {
    dataset_config_version: String,
    formatter: String,
    header: bool,
    unique_rows: bool,
    virtual_schema_name: String,
    requestid: String,
    count: usize,
    #[serde(rename = "Dataset", default)]
    datasets: Vec<XmlDataset>,
}

impl Default for XmlQuery {
    fn default() -> Self {
        XmlQuery {
            virtual_schema_name: "default".into(),
            unique_rows: true,
            count: 0,
            dataset_config_version: "0.6".into(),
            header: true,
            formatter: "TSV".into(),
            requestid: "rust-biomart".into(),
            datasets: vec![],
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(rename = "Dataset")]
pub struct XmlDataset {
    name: String,
    #[serde(rename = "Filter", default)]
    filters: Vec<XmlFilter>,
    #[serde(rename = "Attribute", default)]
    attributes: Vec<XmlAttribute>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(rename = "Filter")]
pub struct XmlFilter {
    name: String,
    value: Option<String>,
    exclude: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(rename = "Attribute")]
pub struct XmlAttribute {
    name: String,
}

impl ToString for XmlQuery {
    fn to_string(&self) -> String {
        // FIXME: serde_xml_rs::to_string does not work atm ( https://github.com/RReverser/serde-xml-rs/issues/99 )
        serde_xml_rs::to_string(self).unwrap()
    }
}

//  let filters = self
//             .filters
//             .iter()
//             .map(|(filter, value)| match value {
//                 FilterOperation::Match(values) => {
//                     let s: String = values.iter().join(",");
//                     XmlFilter {
//                         name: filter.to_string(),
//                         value: s.into(),
//                         exclude: None,
//                     }
//                 }
//                 FilterOperation::Exclude => XmlFilter {
//                     name: filter.to_string(),
//                     value: None,
//                     exclude: Some(true),
//                 },
//                 FilterOperation::Include => XmlFilter {
//                     name: filter.to_string(),
//                     value: None,
//                     exclude: Some(false),
//                 },
//             })
//             .collect();
//
//         let attributes = self
//             .attributes
//             .iter()
//             .map(|attribute| XmlAttribute {
//                 name: attribute.to_string(),
//             })
//             .collect();
//         let dataset = XmlDataset {
//             name: (&self.dataset).to_owned(),
//             filters,
//             attributes,
//         };
//         let mut query = XmlQuery::default();
//         query.datasets.push(dataset);
//         query

pub(crate) fn default_on_error_deserializer<'de, D, T>(d: D) -> Result<T, D::Error>
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

pub(crate) fn bool_from_int<'de, D>(deserializer: D) -> Result<bool, D::Error>
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
pub(crate) struct ServerError;

#[derive(Debug)]
pub(crate) struct StatusError(pub(crate) reqwest::StatusCode);

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
