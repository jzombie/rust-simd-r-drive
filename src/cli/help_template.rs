use indoc::indoc;

// Help text template with placeholder
pub const HELP_TEMPLATE: &str = indoc! {r#"
    Examples:
      # Writing a value explicitly
      %BINARY_NAME% data.bin write mykey "Hello, world!"

      # Writing a value from stdin
      echo "Hello, world!" | %BINARY_NAME% data.bin write mykey
      cat file.txt | %BINARY_NAME% data.bin write mykey

      # Reading a value
      %BINARY_NAME% data.bin read mykey

      # Deleting a key
      %BINARY_NAME% data.bin delete mykey

      # Copying a key to another storage file
      %BINARY_NAME% data.bin copy mykey target_data.bin

      # Moving a key to another storage file (deletes from source)
      %BINARY_NAME% data.bin move mykey target_data.bin

      # Compacting the storage file
      %BINARY_NAME% data.bin compact

      # Displaying storage file info
      %BINARY_NAME% data.bin info
"#};
