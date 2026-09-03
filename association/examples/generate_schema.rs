fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = genohype_association::generate_manifest_schema();
    println!("{}", serde_json::to_string_pretty(&schema)?);
    Ok(())
}
