fn main() {
    let schema = genohype_association::generate_association_page_schema();
    println!("{}", serde_json::to_string_pretty(&schema).unwrap());
}
