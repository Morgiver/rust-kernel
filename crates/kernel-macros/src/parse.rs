//! Hand-written token parsing.
//!
//! This crate has no dependencies, so there is no `syn` to lean on. What
//! follows is the smallest reader that covers the three macros: a cursor over a
//! flat token list, attribute collection, and a handful of string predicates
//! over type text. It parses what the macros document and refuses everything
//! else with a `compile_error!` that names the offending construct — a partial
//! parser that guesses would be worse than one that says no.

use proc_macro::{Delimiter, Group, Ident, Literal, TokenStream, TokenTree};

/// A refusal, rendered as a `compile_error!` invocation at the macro's site.
pub(crate) struct Error {
    message: String,
}

impl Error {
    /// Builds a refusal carrying `message`.
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Renders the refusal as tokens the compiler will report.
    pub(crate) fn into_stream(self) -> TokenStream {
        let mut inner = TokenStream::new();
        inner.extend([TokenTree::Literal(Literal::string(&self.message))]);
        let mut out: TokenStream = "::core::compile_error!"
            .parse()
            .expect("a fixed path parses");
        out.extend([TokenTree::Group(Group::new(Delimiter::Brace, inner))]);
        out
    }
}

/// Result of any parsing step.
pub(crate) type Result<T> = core::result::Result<T, Error>;

/// One `#[...]` attribute, kept both parsed and verbatim.
pub(crate) struct Attribute {
    /// First path segment of the attribute, e.g. `config` or `doc`.
    pub(crate) name: String,
    /// The delimited argument list, when the attribute has one.
    pub(crate) args: Option<Group>,
    /// The attribute exactly as written, for re-emission.
    pub(crate) raw: Vec<TokenTree>,
}

/// A cursor over a flat token list.
pub(crate) struct Cursor {
    tokens: Vec<TokenTree>,
    pos: usize,
}

impl Cursor {
    /// Reads `stream` from the start.
    pub(crate) fn new(stream: TokenStream) -> Self {
        Self {
            tokens: stream.into_iter().collect(),
            pos: 0,
        }
    }

    /// Whether every token has been consumed.
    pub(crate) fn is_empty(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    /// The next token, without consuming it.
    pub(crate) fn peek(&self) -> Option<&TokenTree> {
        self.tokens.get(self.pos)
    }

    /// Consumes and returns the next token.
    pub(crate) fn bump(&mut self) -> Option<TokenTree> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    /// Consumes `name` if it is next.
    pub(crate) fn eat_ident(&mut self, name: &str) -> bool {
        match self.peek() {
            Some(TokenTree::Ident(ident)) if ident.to_string() == name => {
                self.pos += 1;
                true
            }
            _ => false,
        }
    }

    /// Consumes the punctuation `ch` if it is next.
    pub(crate) fn eat_punct(&mut self, ch: char) -> bool {
        match self.peek() {
            Some(TokenTree::Punct(punct)) if punct.as_char() == ch => {
                self.pos += 1;
                true
            }
            _ => false,
        }
    }

    /// Whether the next token is the punctuation `ch`.
    pub(crate) fn at_punct(&self, ch: char) -> bool {
        matches!(self.peek(), Some(TokenTree::Punct(punct)) if punct.as_char() == ch)
    }

    /// Consumes an identifier, or refuses naming what was expected.
    pub(crate) fn ident(&mut self, expected: &str) -> Result<Ident> {
        match self.bump() {
            Some(TokenTree::Ident(ident)) => Ok(ident),
            other => Err(Error::new(format!(
                "expected {expected}, found {}",
                show(&other)
            ))),
        }
    }

    /// Consumes a group with the given delimiter, or refuses.
    pub(crate) fn group(&mut self, delimiter: Delimiter, expected: &str) -> Result<Group> {
        match self.bump() {
            Some(TokenTree::Group(group)) if group.delimiter() == delimiter => Ok(group),
            other => Err(Error::new(format!(
                "expected {expected}, found {}",
                show(&other)
            ))),
        }
    }

    /// Consumes every leading `#[...]` attribute.
    pub(crate) fn attributes(&mut self) -> Result<Vec<Attribute>> {
        let mut out = Vec::new();
        while self.at_punct('#') {
            let pound = self.bump().expect("peeked");
            let group = self.group(Delimiter::Bracket, "an attribute body")?;
            let mut inner = Cursor::new(group.stream());
            let name = inner.ident("an attribute name")?.to_string();
            let args = match inner.peek() {
                Some(TokenTree::Group(group)) => Some(group.clone()),
                _ => None,
            };
            out.push(Attribute {
                name,
                args,
                raw: vec![pound, TokenTree::Group(group)],
            });
        }
        Ok(out)
    }

    /// Consumes a visibility, if one is present, and returns it verbatim.
    pub(crate) fn visibility(&mut self) -> Vec<TokenTree> {
        let mut out = Vec::new();
        if self.eat_ident("pub") {
            out.push(TokenTree::Ident(Ident::new(
                "pub",
                proc_macro::Span::call_site(),
            )));
            if let Some(TokenTree::Group(group)) = self.peek()
                && group.delimiter() == Delimiter::Parenthesis
            {
                out.push(self.bump().expect("peeked"));
            }
        }
        out
    }
}

/// Names a token for a refusal message.
fn show(token: &Option<TokenTree>) -> String {
    match token {
        Some(token) => format!("`{token}`"),
        None => "end of input".to_string(),
    }
}

/// Renders tokens as source text.
pub(crate) fn text<'t>(tokens: impl IntoIterator<Item = &'t TokenTree>) -> String {
    tokens
        .into_iter()
        .cloned()
        .collect::<TokenStream>()
        .to_string()
}

/// Splits `tokens` on top-level occurrences of the punctuation `ch`.
///
/// Top level means outside any delimited group — those are single tokens — and
/// outside any `<...>`, which is not a group and has to be counted by hand.
/// A trailing separator produces no empty final chunk.
pub(crate) fn split(tokens: &[TokenTree], ch: char) -> Vec<Vec<TokenTree>> {
    let mut out: Vec<Vec<TokenTree>> = Vec::new();
    let mut current: Vec<TokenTree> = Vec::new();
    let mut depth = 0i32;
    let mut index = 0usize;
    while index < tokens.len() {
        let token = &tokens[index];
        if let TokenTree::Punct(punct) = token {
            match punct.as_char() {
                '<' => depth += 1,
                '>' => depth -= 1,
                // `->` and `=>`: the `>` is not a closing bracket.
                '-' | '=' => {
                    if let Some(TokenTree::Punct(next)) = tokens.get(index + 1)
                        && next.as_char() == '>'
                    {
                        current.push(token.clone());
                        current.push(tokens[index + 1].clone());
                        index += 2;
                        continue;
                    }
                }
                _ => {}
            }
            if punct.as_char() == ch && depth <= 0 {
                out.push(core::mem::take(&mut current));
                index += 1;
                continue;
            }
        }
        current.push(token.clone());
        index += 1;
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// Splits a type's text into its path head and its generic arguments.
///
/// `Arc < dyn Surface >` yields `("Arc", ["dyn Surface"])`. A type with no
/// argument list yields an empty argument vector. Argument text keeps its
/// spacing so that it can be emitted again unchanged.
pub(crate) fn generics(ty: &str) -> (String, Vec<String>) {
    let Some(open) = ty.find('<') else {
        return (ty.trim().to_string(), Vec::new());
    };
    let head = ty[..open].trim().to_string();
    let mut depth = 0i32;
    let mut close = ty.len();
    let mut arg_start = open + 1;
    let mut args: Vec<String> = Vec::new();
    let mut previous = ' ';
    for (index, character) in ty.char_indices().skip_while(|(i, _)| *i < open) {
        match character {
            '<' => depth += 1,
            // The `>` of `->` or `=>` closes nothing.
            '>' if previous == '-' || previous == '=' => {}
            '>' => {
                depth -= 1;
                if depth == 0 {
                    close = index;
                    break;
                }
            }
            ',' if depth == 1 => {
                args.push(ty[arg_start..index].trim().to_string());
                arg_start = index + 1;
            }
            _ => {}
        }
        if !character.is_whitespace() {
            previous = character;
        }
    }
    let tail = ty[arg_start.min(close)..close].trim();
    if !tail.is_empty() {
        args.push(tail.to_string());
    }
    (head, args)
}

/// Last segment of a path, whitespace removed: `std :: sync :: Arc` -> `Arc`.
pub(crate) fn last_segment(path: &str) -> String {
    let compact: String = path.chars().filter(|c| !c.is_whitespace()).collect();
    compact.rsplit("::").next().unwrap_or(&compact).to_string()
}

/// Whether a type is `Option<...>` under any of its usual paths.
pub(crate) fn is_option(ty: &str) -> bool {
    let (head, args) = generics(ty);
    !args.is_empty() && last_segment(&head) == "Option" && head_is_std(&head, "option")
}

/// Whether a path is either bare or rooted in `std`, `core` or `alloc`.
fn head_is_std(head: &str, module: &str) -> bool {
    let compact: String = head.chars().filter(|c| !c.is_whitespace()).collect();
    let compact = compact.trim_start_matches("::");
    let segments: Vec<&str> = compact.split("::").collect();
    match segments.len() {
        1 => true,
        3 => matches!(segments[0], "std" | "core" | "alloc") && segments[1] == module,
        _ => false,
    }
}

/// Reads `crate = <path>` out of a macro's argument list.
///
/// Every macro emits absolute paths, so a crate renamed in `Cargo.toml`, or
/// reached through a re-export, needs to say where it lives. Returns `None`
/// when the argument list is empty.
pub(crate) fn crate_path(args: TokenStream, default: &str) -> Result<String> {
    let mut cursor = Cursor::new(args);
    if cursor.is_empty() {
        return Ok(default.to_string());
    }
    if !cursor.eat_ident("crate") {
        return Err(Error::new("the only accepted argument is `crate = <path>`"));
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
    Ok(text(&path))
}
