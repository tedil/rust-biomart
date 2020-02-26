use serde::{Deserialize, Serialize};

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
