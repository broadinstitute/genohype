use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "static/association-viewer"]
struct AssociationViewerAssets;

pub(crate) struct EmbeddedAsset {
    pub(crate) bytes: Vec<u8>,
    pub(crate) media_type: String,
    pub(crate) immutable: bool,
}

pub(crate) fn viewer_index() -> Option<EmbeddedAsset> {
    embedded("dist/index.html", false).or_else(|| embedded("index.html", false))
}

pub(crate) fn viewer_asset(path: &str) -> Option<EmbeddedAsset> {
    if !is_safe_asset_path(path) {
        return None;
    }
    embedded(&format!("dist/assets/{path}"), is_checksum_addressed(path))
}

fn embedded(path: &str, immutable: bool) -> Option<EmbeddedAsset> {
    let file = AssociationViewerAssets::get(path)?;
    Some(EmbeddedAsset {
        bytes: file.data.into_owned(),
        media_type: mime_guess::from_path(path)
            .first_or_octet_stream()
            .essence_str()
            .to_string(),
        immutable,
    })
}

pub(crate) fn is_safe_asset_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains(['\\', '\0', '%'])
        && path
            .split('/')
            .all(|component| !component.is_empty() && component != "." && component != "..")
}

pub(crate) fn is_checksum_addressed(path: &str) -> bool {
    let Some(name) = path.rsplit('/').next() else {
        return false;
    };
    let Some((stem, extension)) = name.rsplit_once('.') else {
        return false;
    };
    if !matches!(extension, "js" | "css" | "map" | "woff" | "woff2") {
        return false;
    }
    if stem.len() < 9 {
        return false;
    }
    let hash_start = stem.len() - 8;
    stem[..hash_start].ends_with('-')
        && stem[hash_start..]
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asset_paths_are_confined() {
        for path in ["", ".", "..", "a/../b", "a//b", "a\\b", "a%2fb", "a\0b"] {
            assert!(!is_safe_asset_path(path), "{path:?}");
        }
        assert!(is_safe_asset_path("index-C-b9egNF.js"));
    }

    #[test]
    fn only_vite_hash_names_are_immutable() {
        assert!(is_checksum_addressed("index-Cb9egNF1.js"));
        assert!(is_checksum_addressed("index-C-b9egNF.js"));
        assert!(is_checksum_addressed("nested/app-0123abcd.css"));
        assert!(!is_checksum_addressed("index.js"));
        assert!(!is_checksum_addressed("app-short.js"));
        assert!(!is_checksum_addressed("image-0123abcd.png"));
    }
}
