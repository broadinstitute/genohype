fn main() {
    let schema = genohype_association::generate_region_overlay_schema();
    println!("{}", serde_json::to_string_pretty(&schema).unwrap());
}
