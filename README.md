# rust-biomart
A simple rust client for biomart.

# Examples
- List available marts:
    ```rust
    use rust_biomart::MartClient;
    let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
    let marts = mart_client.marts()?;
    for mart in &marts {
       println!("{}", mart.name());
    }
    ```
- List available datasets for a Mart:
    ```rust
    use rust_biomart::MartClient;
    let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
    let datasets = mart_client.datasets("ENSEMBL_MART_ENSEMBL")?;
    for info in &datasets {
        println!("{}: {}", info.dataset(), info.description());
    }
    ```
- List available filters for a dataset:
    ```rust
    use rust_biomart::MartClient;
    let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
    let filters = mart_client.filters("ENSEMBL_MART_ENSEMBL", "hsapiens_gene_ensembl")?;
    for filter in &filters {
        println!("{}: {}", filter.name(), filter.description());
    }
    ```
- List available attributes for a dataset:
    ```rust
    use rust_biomart::MartClient;
    let mart_client = MartClient::new("http://ensembl.org:80/biomart/martservice");
    let attributes = mart_client.attributes("ENSEMBL_MART_ENSEMBL", "hsapiens_gene_ensembl")?;
    for attribute in &attributes {
        println!("{}: {}", attribute.name(), attribute.description());
    }
    ```
- Perform a query:
    ```rust
    use rust_biomart::MartClient;
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

    let response = mart_client.query(&query).unwrap();  
    assert_eq!(
        "AFFY HG U133 Plus 2 probe	NCBI gene ID
        209310_s_at	837
        207500_at	838
        202763_at	836
        ",
        response.raw()
    );
  
    // Since the response is in TSV format, we also provide
    // accessors for the header and records (via rust-csv):
    let header = response.header().unwrap();
    let records = response.records();
    ```
