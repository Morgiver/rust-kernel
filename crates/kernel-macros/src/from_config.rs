//! `#[derive(FromConfig)]`.
//!
//! Expands to the `impl FromConfig` a user would write by hand: one read per
//! field, from the node under the field's own name, with the field name pushed
//! onto the error path so a failure names the leaf and not the struct.

use proc_macro::{Delimiter, TokenStream, TokenTree};

use crate::parse::{Attribute, Cursor, Error, Result, is_option, text};

/// One field, reduced to what the expansion needs.
struct Field {
    /// Identifier of the field.
    name: String,
    /// Key read from the configuration node.
    key: String,
    /// Type text, re-emitted verbatim in the qualified call.
    ty: String,
    /// Whether absence falls back to `Default::default()`.
    default: bool,
}

/// Expands the derive, or returns the refusal to report.
pub(crate) fn expand(input: TokenStream) -> Result<TokenStream> {
    let mut cursor = Cursor::new(input);
    let attributes = cursor.attributes()?;
    let krate = crate_path(&attributes)?;
    let _ = cursor.visibility();

    if cursor.eat_ident("enum") || cursor.eat_ident("union") {
        return Err(Error::new(
            "FromConfig derives on a struct of named fields only",
        ));
    }
    if !cursor.eat_ident("struct") {
        return Err(Error::new("expected `struct`"));
    }
    let name = cursor.ident("the struct name")?.to_string();
    if cursor.at_punct('<') {
        return Err(Error::new(format!(
            "`{name}` is generic; FromConfig derives on a non-generic struct only, \
             so write the impl by hand"
        )));
    }
    match cursor.peek() {
        Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Parenthesis => {
            return Err(Error::new(format!(
                "`{name}` is a tuple struct; a field is read from the node under its own \
                 name, and a positional field has none"
            )));
        }
        Some(TokenTree::Punct(punct)) if punct.as_char() == ';' => {
            return Err(Error::new(format!(
                "`{name}` has no field to read; FromConfig derives on a struct of named \
                 fields"
            )));
        }
        _ => {}
    }
    let body = cursor.group(Delimiter::Brace, "a braced list of named fields")?;
    let fields = parse_fields(body.stream())?;

    Ok(render(&krate, &name, &fields))
}

/// Reads `#[config(crate = <path>)]` off the struct.
fn crate_path(attributes: &[Attribute]) -> Result<String> {
    for attribute in attributes {
        if attribute.name != "config" {
            continue;
        }
        let Some(args) = &attribute.args else {
            return Err(Error::new("`#[config]` needs an argument list"));
        };
        let mut cursor = Cursor::new(args.stream());
        if !cursor.eat_ident("crate") {
            return Err(Error::new(
                "the only struct-level option is `#[config(crate = <path>)]`",
            ));
        }
        if !cursor.eat_punct('=') {
            return Err(Error::new("expected `=` after `crate`"));
        }
        let mut path = Vec::new();
        while let Some(token) = cursor.bump() {
            path.push(token);
        }
        if path.is_empty() {
            return Err(Error::new("expected a path after `crate =`"));
        }
        return Ok(text(&path));
    }
    Ok("::kernel_core".to_string())
}

/// Reads the field list out of the struct body.
fn parse_fields(stream: TokenStream) -> Result<Vec<Field>> {
    let mut cursor = Cursor::new(stream);
    let mut fields = Vec::new();
    while !cursor.is_empty() {
        let attributes = cursor.attributes()?;
        let _ = cursor.visibility();
        let name = cursor.ident("a field name")?.to_string();
        if !cursor.eat_punct(':') {
            return Err(Error::new(format!(
                "field `{name}` has no type; FromConfig derives on named fields only"
            )));
        }
        let mut ty = Vec::new();
        let mut depth = 0i32;
        while let Some(token) = cursor.peek() {
            if let TokenTree::Punct(punct) = token {
                match punct.as_char() {
                    '<' => depth += 1,
                    '>' => depth -= 1,
                    ',' if depth <= 0 => break,
                    _ => {}
                }
            }
            ty.push(cursor.bump().expect("peeked"));
        }
        cursor.eat_punct(',');
        let ty = text(&ty);
        let (key, default) = field_options(&attributes, &name)?;
        fields.push(Field {
            name,
            key,
            ty,
            default,
        });
    }
    Ok(fields)
}

/// Reads `#[config(rename = "...")]` and `#[config(default)]` off a field.
fn field_options(attributes: &[Attribute], name: &str) -> Result<(String, bool)> {
    let mut key = format!("{name:?}");
    let mut default = false;
    for attribute in attributes {
        if attribute.name != "config" {
            continue;
        }
        let Some(args) = &attribute.args else {
            return Err(Error::new(format!(
                "`#[config]` on `{name}` needs an argument list"
            )));
        };
        let mut cursor = Cursor::new(args.stream());
        while !cursor.is_empty() {
            if cursor.eat_ident("default") {
                default = true;
            } else if cursor.eat_ident("rename") {
                if !cursor.eat_punct('=') {
                    return Err(Error::new(format!(
                        "expected `=` after `rename` on `{name}`"
                    )));
                }
                match cursor.bump() {
                    Some(TokenTree::Literal(literal)) => key = literal.to_string(),
                    _ => {
                        return Err(Error::new(format!(
                            "`rename` on `{name}` takes a string literal"
                        )));
                    }
                }
            } else {
                return Err(Error::new(format!(
                    "unknown `#[config(...)]` option on `{name}`; \
                     the field options are `rename = \"...\"` and `default`"
                )));
            }
            cursor.eat_punct(',');
        }
    }
    Ok((key, default))
}

/// Emits the impl.
fn render(krate: &str, name: &str, fields: &[Field]) -> TokenStream {
    let mut body = String::new();
    if fields.is_empty() {
        body.push_str("let _ = node;\n");
    } else {
        body.push_str(&nesting_helper(krate));
    }
    body.push_str("::core::result::Result::Ok(Self {\n");
    for field in fields {
        let absent = if field.default {
            "::core::default::Default::default()".to_string()
        } else if is_option(&field.ty) {
            "::core::option::Option::None".to_string()
        } else {
            format!(
                "return ::core::result::Result::Err({krate}::error::ConfigError::missing({key}))",
                key = field.key
            )
        };
        body.push_str(&format!(
            "{name}: match {krate}::config::ConfigNode::get(node, {key}) {{
                ::core::option::Option::Some(__child) =>
                    match <{ty} as {krate}::config::FromConfig>::from_config(__child) {{
                        ::core::result::Result::Ok(__value) => __value,
                        ::core::result::Result::Err(__error) =>
                            return ::core::result::Result::Err(__nest({key}, __error)),
                    }},
                ::core::option::Option::None => {absent},
            }},\n",
            name = field.name,
            key = field.key,
            ty = field.ty,
        ));
    }
    body.push_str("})\n");

    let rendered = format!(
        "impl {krate}::config::FromConfig for {name} {{
            fn from_config(node: &{krate}::config::ConfigNode)
                -> ::core::result::Result<Self, {krate}::error::ConfigError>
            {{
                {body}
            }}
        }}"
    );
    rendered.parse().expect("the derive emits valid Rust")
}

/// The path-prefixing helper, emitted inside the generated method.
///
/// `ConfigError` carries a path relative to the node it was produced from, and
/// the caller is what knows where that node lives. The kernel's own containers
/// do exactly this; the helper is local to the method so the expansion adds no
/// item to the user's namespace.
fn nesting_helper(krate: &str) -> String {
    format!(
        "fn __nest(segment: &str, error: {krate}::error::ConfigError)
            -> {krate}::error::ConfigError
        {{
            let path = if {krate}::error::ConfigError::path(&error).is_empty() {{
                ::std::string::ToString::to_string(segment)
            }} else {{
                ::std::format!(\"{{}}.{{}}\", segment, {krate}::error::ConfigError::path(&error))
            }};
            let rebuilt = match {krate}::error::ConfigError::kind(&error) {{
                {krate}::error::ConfigErrorKind::Missing =>
                    ::core::option::Option::Some({krate}::error::ConfigError::missing(path)),
                {krate}::error::ConfigErrorKind::TypeMismatch {{ expected, found }} =>
                    ::core::option::Option::Some(
                        {krate}::error::ConfigError::type_mismatch(path, *expected, *found),
                    ),
                {krate}::error::ConfigErrorKind::Invalid(detail) =>
                    ::core::option::Option::Some({krate}::error::ConfigError::invalid(
                        path,
                        ::std::clone::Clone::clone(detail),
                    )),
                {krate}::error::ConfigErrorKind::Source(_) => ::core::option::Option::None,
            }};
            match rebuilt {{
                ::core::option::Option::Some(error) => error,
                ::core::option::Option::None => error,
            }}
        }}\n"
    )
}
