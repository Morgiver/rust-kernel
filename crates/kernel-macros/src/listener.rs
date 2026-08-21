//! `#[listener]`.
//!
//! Implements `Listener<E>` from a method taking `&mut E`, so the handler reads
//! as an ordinary method and the boxed-future signature is written once, here,
//! instead of once per event type.

use proc_macro::{Delimiter, TokenStream, TokenTree};

use crate::parse::{Cursor, Error, Result, crate_path, text};

/// One handler found in the impl block.
struct Handler {
    /// Method name.
    name: String,
    /// The event type behind `&mut E`.
    event: String,
    /// Whether the method also takes the listener context.
    takes_context: bool,
    /// Whether the method is `async`.
    is_async: bool,
}

/// Expands the attribute, or returns the refusal to report.
pub(crate) fn expand(args: TokenStream, input: TokenStream) -> Result<TokenStream> {
    let krate = crate_path(args, "::kernel")?;
    let mut cursor = Cursor::new(input.clone());

    let _ = cursor.attributes()?;
    if !cursor.eat_ident("impl") {
        return Err(Error::new(
            "`#[listener]` applies to an inherent `impl` block whose methods handle events",
        ));
    }
    if cursor.at_punct('<') {
        return Err(Error::new(
            "`#[listener]` does not read a generic `impl` block; write those impls by hand",
        ));
    }

    let mut self_type = Vec::new();
    loop {
        match cursor.peek() {
            Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Brace => break,
            Some(TokenTree::Ident(ident)) if ident.to_string() == "for" => {
                return Err(Error::new(
                    "`#[listener]` applies to an inherent `impl` block, not a trait impl",
                ));
            }
            Some(_) => self_type.push(cursor.bump().expect("peeked")),
            None => return Err(Error::new("expected the body of the `impl` block")),
        }
    }
    let body = cursor.group(Delimiter::Brace, "the body of the `impl` block")?;
    let self_type = text(&self_type);
    let handlers = parse_handlers(body.stream())?;
    if handlers.is_empty() {
        return Err(Error::new(
            "`#[listener]` found no handler; a handler is a method taking `&self` and \
             `&mut E`",
        ));
    }

    let mut out = input;
    for handler in &handlers {
        out.extend(render(&krate, &self_type, handler));
    }
    Ok(out)
}

/// Reads every method of the block, refusing anything that is not a handler.
fn parse_handlers(stream: TokenStream) -> Result<Vec<Handler>> {
    let mut cursor = Cursor::new(stream);
    let mut out = Vec::new();
    while !cursor.is_empty() {
        let _ = cursor.attributes()?;
        let _ = cursor.visibility();
        let is_async = cursor.eat_ident("async");
        if !cursor.eat_ident("fn") {
            return Err(Error::new(
                "`#[listener]` reads a block of handler methods only; keep helpers, \
                 constants and associated types in a separate `impl` block, where nothing \
                 can silently fail to become a listener",
            ));
        }
        let name = cursor.ident("a method name")?.to_string();
        let list = cursor.group(Delimiter::Parenthesis, "a parameter list")?;
        // The return type and the body are left untouched; the original block is
        // re-emitted verbatim.
        while let Some(token) = cursor.peek() {
            let done =
                matches!(token, TokenTree::Group(group) if group.delimiter() == Delimiter::Brace);
            cursor.bump();
            if done {
                break;
            }
        }
        let tokens: Vec<TokenTree> = list.stream().into_iter().collect();
        let parameters = crate::parse::split(&tokens, ',');
        out.push(handler(&name, is_async, &parameters)?);
    }
    Ok(out)
}

/// Checks one method's parameter list and reads the event type out of it.
fn handler(name: &str, is_async: bool, parameters: &[Vec<TokenTree>]) -> Result<Handler> {
    let shape = format!(
        "`{name}` is not a handler: a handler takes `&self`, then `&mut E`, and \
         optionally a `&ListenerContext<'_>`"
    );
    if parameters.len() < 2 || parameters.len() > 3 {
        return Err(Error::new(shape));
    }
    if text(&parameters[0]).replace(' ', "") != "&self" {
        return Err(Error::new(shape));
    }
    let second = text(&parameters[1]);
    let Some(colon) = second.find(':') else {
        return Err(Error::new(shape));
    };
    let ty = second[colon + 1..].trim();
    let Some(event) = ty.strip_prefix('&') else {
        return Err(Error::new(shape));
    };
    let Some(event) = event.trim().strip_prefix("mut ") else {
        return Err(Error::new(shape));
    };
    Ok(Handler {
        name: name.to_string(),
        event: event.trim().to_string(),
        takes_context: parameters.len() == 3,
        is_async,
    })
}

/// Emits one `Listener<E>` impl.
fn render(krate: &str, self_type: &str, handler: &Handler) -> TokenStream {
    let context = if handler.takes_context {
        ", __context"
    } else {
        ""
    };
    let call = format!("Self::{name}(self, __event{context})", name = handler.name);
    let future = if handler.is_async {
        format!("::std::boxed::Box::pin({call})")
    } else {
        format!("::std::boxed::Box::pin(async move {{ {call} }})")
    };
    format!(
        "impl {krate}::Listener<{event}> for {self_type} {{
            fn on_event<'__handler>(
                &'__handler self,
                __event: &'__handler mut {event},
                __context: &'__handler {krate}::ListenerContext<'__handler>,
            ) -> {krate}::core::BoxFuture<
                '__handler,
                ::core::result::Result<{krate}::core::Flow, {krate}::core::ListenerError>,
            > {{
                let _ = __context;
                {future}
            }}
        }}",
        event = handler.event,
    )
    .parse()
    .expect("the attribute emits valid Rust")
}
