use indoc::indoc;

// Help text template with placeholder
pub const HELP_TEMPLATE: &str = indoc! {r#"
    Examples:
      # Listen on a random internal port
      %BINARY_NAME% data.bin

      # Listen on a pre-specified socket address
      %BINARY_NAME% data.bin --listen 127.0.0.1:7000
"#};
