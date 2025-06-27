use indoc::indoc;

// Help text template with placeholder for the binary name.
pub const HELP_TEMPLATE: &str = indoc! {r#"
    Examples:
      # Start server with a storage file, listening on a random port on 127.0.0.1
      %BINARY_NAME% data.bin

      # Listen on a specific host and port
      %BINARY_NAME% data.bin --host 0.0.0.0 --port 7000

      # Listen on a specific port on the default host (127.0.0.1)
      %BINARY_NAME% data.bin --port 7000
"#};
