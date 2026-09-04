pub(crate) fn open(url: &str) -> std::io::Result<()> {
    webbrowser::open(url)
}
