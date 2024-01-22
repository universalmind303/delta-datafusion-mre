use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    datasource::TableProvider,
    execution::context::SessionContext,
};
use deltalake::{
    kernel::{DataType as DeltaDataType, PrimitiveType},
    logstore::default_logstore,
    operations::{create::CreateBuilder, write::WriteBuilder},
    protocol::SaveMode,
    storage::object_store::local::LocalFileSystem,
};
use url::Url;
#[tokio::main]
async fn main() {
    let path = tempfile::tempdir().unwrap();
    let path = path.into_path();

    let file_store = LocalFileSystem::new_with_prefix(path.clone()).unwrap();
    let log_store = default_logstore(
        Arc::new(file_store),
        &Url::from_file_path(path.clone()).unwrap(),
        &Default::default(),
    );

    let tbl = CreateBuilder::new()
        .with_log_store(log_store.clone())
        .with_save_mode(SaveMode::Overwrite)
        .with_table_name("test")
        .with_column(
            "id",
            DeltaDataType::Primitive(PrimitiveType::Integer),
            true,
            None,
        );
    let tbl = tbl.await.unwrap();
    let ctx = SessionContext::new();
    let plan = ctx
        .sql("SELECT 1 as id")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let write_builder = WriteBuilder::new(log_store, tbl.state);
    let _ = write_builder
        .with_input_execution_plan(plan)
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();

    let table = deltalake::open_table(path.to_str().unwrap()).await.unwrap();
    let prov: Arc<dyn TableProvider> = Arc::new(table);
    ctx.register_table("test", prov).unwrap();
    let mut batches = ctx
        .sql("SELECT * FROM test")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let batch = batches.pop().unwrap();

    let expected_schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
    assert_eq!(batch.schema().as_ref(), &expected_schema);
}
