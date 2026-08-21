//! `#[provider]`.
//!
//! Turns a constructor into a function that returns the `Provider` binding it,
//! and derives `requires` from the constructor's parameter list.
//!
//! `requires` is declarative because the language offers no introspection:
//! nothing can look inside a build closure and see what it will ask the
//! container for. The container's debug guard catches a list that has drifted
//! from what `build` resolves, but it catches it at run time, on the code path
//! that happens to run. Deriving the list from the signature removes the drift
//! instead of reporting it: the parameters are what the generated closure
//! resolves, so the declaration cannot disagree with the resolution.

use proc_macro::{Delimiter, Group, TokenStream, TokenTree};

use crate::parse::{Attribute, Cursor, Error, Result, crate_path, generics, last_segment, text};

/// What one constructor parameter resolves to.
enum Source {
    /// `Arc<C>`, bound under no name.
    Contract(String),
    /// `Arc<C>`, bound under a name; the name is kept as its literal.
    Named(String, String),
    /// `Vec<Arc<C>>`, every implementation of `C` in registration order.
    All(String),
    /// `&ConfigTree`, the tree frozen at the end of phase one.
    Config,
}

/// One parameter, reduced to what the expansion needs.
struct Parameter {
    /// The parameter as written, minus the macro's own attributes.
    declaration: String,
    /// Where its value comes from.
    source: Source,
}

/// Expands the attribute, or returns the refusal to report.
pub(crate) fn expand(args: TokenStream, input: TokenStream) -> Result<TokenStream> {
    let krate = crate_path(args, "::kernel")?;
    let mut cursor = Cursor::new(input);

    let attributes = cursor.attributes()?;
    let visibility = text(&cursor.visibility());
    let is_async = cursor.eat_ident("async");
    if !cursor.eat_ident("fn") {
        return Err(Error::new(
            "`#[provider]` applies to a constructor function",
        ));
    }
    let name = cursor.ident("the constructor name")?.to_string();
    if cursor.at_punct('<') {
        return Err(Error::new(format!(
            "`{name}` is generic; a provider binds one contract, so write it without \
             type parameters"
        )));
    }
    let list = cursor.group(Delimiter::Parenthesis, "a parameter list")?;
    let parameters = parse_parameters(&list)?;

    let mut return_tokens = Vec::new();
    if cursor.eat_punct('-') {
        if !cursor.eat_punct('>') {
            return Err(Error::new("expected `->` before the return type"));
        }
        while let Some(token) = cursor.peek() {
            if matches!(token, TokenTree::Group(group) if group.delimiter() == Delimiter::Brace) {
                break;
            }
            return_tokens.push(cursor.bump().expect("peeked"));
        }
    }
    let body = cursor.group(Delimiter::Brace, "the constructor body")?;
    let (contract, fallible) = parse_return(&text(&return_tokens), &name)?;

    Ok(render(
        &krate,
        &Constructor {
            attributes,
            visibility,
            is_async,
            name,
            parameters,
            return_type: text(&return_tokens),
            body,
            contract,
            fallible,
        },
    ))
}

/// Everything the expansion reads off the constructor.
struct Constructor {
    attributes: Vec<Attribute>,
    visibility: String,
    is_async: bool,
    name: String,
    parameters: Vec<Parameter>,
    return_type: String,
    body: Group,
    contract: String,
    fallible: Option<String>,
}

/// Reads the parameter list.
fn parse_parameters(list: &Group) -> Result<Vec<Parameter>> {
    let tokens: Vec<TokenTree> = list.stream().into_iter().collect();
    let mut out = Vec::new();
    for chunk in crate::parse::split(&tokens, ',') {
        let mut cursor = Cursor::new(chunk.iter().cloned().collect::<TokenStream>());
        let attributes = cursor.attributes()?;
        let mut kept = Vec::new();
        while let Some(token) = cursor.bump() {
            kept.push(token);
        }
        let declaration = text(&kept);
        let Some(colon) = separator(&kept) else {
            return Err(Error::new(format!(
                "parameter `{declaration}` has no type; `#[provider]` reads what a \
                 constructor resolves from its parameter types"
            )));
        };
        let ty = text(&kept[colon + 1..]);
        let source = classify(&ty, &attributes, &declaration)?;
        out.push(Parameter {
            declaration,
            source,
        });
    }
    Ok(out)
}

/// Position of the `:` separating a parameter's pattern from its type.
///
/// The `:` of a path such as `std::sync::Arc` is joint with the next one, which
/// is what tells the two apart.
fn separator(tokens: &[TokenTree]) -> Option<usize> {
    let mut index = 0;
    while index < tokens.len() {
        if let TokenTree::Punct(punct) = &tokens[index]
            && punct.as_char() == ':'
        {
            if punct.spacing() == proc_macro::Spacing::Joint {
                index += 2;
                continue;
            }
            return Some(index);
        }
        index += 1;
    }
    None
}

/// Decides where a parameter's value comes from, from its type.
fn classify(ty: &str, attributes: &[Attribute], declaration: &str) -> Result<Source> {
    let named = named_option(attributes, declaration)?;

    if is_config_tree(ty) {
        if named.is_some() {
            return Err(Error::new(format!(
                "`#[named]` on `{declaration}`: the configuration tree is not a contract"
            )));
        }
        return Ok(Source::Config);
    }

    let (head, args) = generics(ty);
    match (last_segment(&head).as_str(), args.len()) {
        ("Arc", 1) => Ok(match named {
            Some(name) => Source::Named(args[0].clone(), name),
            None => Source::Contract(args[0].clone()),
        }),
        ("Vec", 1) => {
            let (inner_head, inner_args) = generics(&args[0]);
            if last_segment(&inner_head) != "Arc" || inner_args.len() != 1 {
                return Err(unsupported(declaration));
            }
            if named.is_some() {
                return Err(Error::new(format!(
                    "`#[named]` on `{declaration}`: a collection takes every implementation \
                     of the contract, named ones included"
                )));
            }
            Ok(Source::All(inner_args[0].clone()))
        }
        _ => Err(unsupported(declaration)),
    }
}

/// The refusal a parameter of an unreadable type gets.
fn unsupported(declaration: &str) -> Error {
    Error::new(format!(
        "`#[provider]` cannot read what `{declaration}` resolves to. A parameter is \
         `Arc<C>` for one implementation, `Vec<Arc<C>>` for every implementation, or \
         `&ConfigTree` for the configuration. Anything else — the container itself \
         above all — hides what the build resolves, which is exactly what this \
         attribute exists to expose: write that provider by hand."
    ))
}

/// Reads `#[named("...")]` off a parameter.
fn named_option(attributes: &[Attribute], declaration: &str) -> Result<Option<String>> {
    let mut found = None;
    for attribute in attributes {
        if attribute.name != "named" {
            return Err(Error::new(format!(
                "unknown attribute `{}` on `{declaration}`; the only parameter attribute \
                 is `#[named(\"...\")]`",
                attribute.name
            )));
        }
        let Some(args) = &attribute.args else {
            return Err(Error::new(format!(
                "`#[named]` on `{declaration}` takes a string literal"
            )));
        };
        let mut cursor = Cursor::new(args.stream());
        match cursor.bump() {
            Some(TokenTree::Literal(literal)) if cursor.is_empty() => {
                found = Some(literal.to_string());
            }
            _ => {
                return Err(Error::new(format!(
                    "`#[named]` on `{declaration}` takes a single string literal"
                )));
            }
        }
    }
    Ok(found)
}

/// Whether a parameter type is a borrow of the configuration tree.
fn is_config_tree(ty: &str) -> bool {
    let Some(rest) = ty.trim().strip_prefix('&') else {
        return false;
    };
    let rest = rest.trim();
    let rest = match rest.strip_prefix('\'') {
        Some(tail) => tail.trim_start_matches(|c: char| c.is_alphanumeric() || c == '_'),
        None => rest,
    };
    let (head, args) = generics(rest);
    args.is_empty() && last_segment(&head) == "ConfigTree"
}

/// Reads the contract, and the error type when the constructor is fallible.
fn parse_return(return_type: &str, name: &str) -> Result<(String, Option<String>)> {
    if return_type.trim().is_empty() {
        return Err(Error::new(format!(
            "`{name}` returns nothing; a constructor returns `Arc<C>`, or \
             `Result<Arc<C>, E>` when it can fail"
        )));
    }
    let (head, args) = generics(return_type);
    match (last_segment(&head).as_str(), args.len()) {
        ("Arc", 1) => Ok((args[0].clone(), None)),
        ("Result", 2) => {
            let (inner_head, inner_args) = generics(&args[0]);
            if last_segment(&inner_head) != "Arc" || inner_args.len() != 1 {
                return Err(bad_return(name, return_type));
            }
            Ok((inner_args[0].clone(), Some(args[1].clone())))
        }
        _ => Err(bad_return(name, return_type)),
    }
}

/// The refusal an unreadable return type gets.
fn bad_return(name: &str, return_type: &str) -> Error {
    Error::new(format!(
        "`{name}` returns `{return_type}`; a constructor returns `Arc<C>`, or \
         `Result<Arc<C>, E>` when it can fail. `C` is the contract the provider binds"
    ))
}

/// Emits the provider function.
fn render(krate: &str, constructor: &Constructor) -> TokenStream {
    let contract = &constructor.contract;
    let mut resolutions = String::new();
    let mut arguments = Vec::new();
    let mut requires = Vec::new();

    for (index, parameter) in constructor.parameters.iter().enumerate() {
        let binding = format!("__argument{index}");
        arguments.push(binding.clone());
        match &parameter.source {
            Source::Config => {
                resolutions.push_str(&format!(
                    "let {binding} = {krate}::Container::config(__container);\n"
                ));
            }
            Source::Contract(inner) => {
                requires.push(format!("{krate}::core::ContractRef::of::<{inner}>()"));
                resolutions.push_str(&resolve(
                    krate,
                    &binding,
                    &format!("{krate}::Container::get::<{inner}>(__container)"),
                    contract,
                ));
            }
            Source::Named(inner, name) => {
                requires.push(format!(
                    "{krate}::core::ContractRef::named::<{inner}>({name})"
                ));
                resolutions.push_str(&resolve(
                    krate,
                    &binding,
                    &format!("{krate}::Container::get_named::<{inner}>(__container, {name})"),
                    contract,
                ));
            }
            Source::All(inner) => {
                requires.push(format!("{krate}::core::ContractRef::of::<{inner}>()"));
                resolutions.push_str(&resolve(
                    krate,
                    &binding,
                    &format!("{krate}::Container::get_all::<{inner}>(__container)"),
                    contract,
                ));
            }
        }
    }

    let call = format!(
        "__construct({}){}",
        arguments.join(", "),
        if constructor.is_async { ".await" } else { "" }
    );
    let outcome = match &constructor.fallible {
        None => format!("::core::result::Result::Ok({call})"),
        Some(error) if last_segment(error) == "BuildError" => call,
        Some(_) => format!(
            "match {call} {{
                ::core::result::Result::Ok(__value) => ::core::result::Result::Ok(__value),
                ::core::result::Result::Err(__error) => ::core::result::Result::Err(
                    {krate}::core::BuildError::new(
                        {krate}::core::ContractRef::of::<{contract}>().type_name(),
                        ::std::boxed::Box::new(__error),
                    ),
                ),
            }}"
        ),
    };

    let signature = format!(
        "{}fn __construct({}) {}",
        if constructor.is_async { "async " } else { "" },
        constructor
            .parameters
            .iter()
            .map(|parameter| parameter.declaration.clone())
            .collect::<Vec<_>>()
            .join(", "),
        if constructor.return_type.trim().is_empty() {
            String::new()
        } else {
            format!("-> {}", constructor.return_type)
        },
    );

    let requires_call = if requires.is_empty() {
        String::new()
    } else {
        format!(".requires([{}])", requires.join(", "))
    };

    let mut out = TokenStream::new();
    for attribute in &constructor.attributes {
        out.extend(attribute.raw.iter().cloned());
    }
    out.extend(
        format!(
            "{visibility} fn {name}() -> {krate}::Provider<{contract}>",
            visibility = constructor.visibility,
            name = constructor.name,
        )
        .parse::<TokenStream>()
        .expect("the attribute emits valid Rust"),
    );

    let mut inner = TokenStream::new();
    inner.extend(
        signature
            .parse::<TokenStream>()
            .expect("the constructor signature round-trips"),
    );
    inner.extend([TokenTree::Group(constructor.body.clone())]);
    inner.extend(
        format!(
            "let __provider: {krate}::Provider<{contract}> =
                 {krate}::Provider::from_fn(|__container| {{
                     ::std::boxed::Box::pin(async move {{
                         {resolutions}
                         {outcome}
                     }})
                 }});
             __provider{requires_call}"
        )
        .parse::<TokenStream>()
        .expect("the attribute emits valid Rust"),
    );
    out.extend([TokenTree::Group(Group::new(Delimiter::Brace, inner))]);
    out
}

/// One resolution, with the container error wrapped into a build failure.
fn resolve(krate: &str, binding: &str, call: &str, contract: &str) -> String {
    format!(
        "let {binding} = match {call}.await {{
            ::core::result::Result::Ok(__value) => __value,
            ::core::result::Result::Err(__error) => return ::core::result::Result::Err(
                {krate}::core::BuildError::new(
                    {krate}::core::ContractRef::of::<{contract}>().type_name(),
                    ::std::boxed::Box::new(__error),
                ),
            ),
        }};\n"
    )
}
