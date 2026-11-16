//! Bounded SYSTEM subset of Tera 2: data expressions, conditions and known Agent lists.
use crate::Error;
use serde_json::Value;
#[cfg(test)]
mod tests;

const MAX_TAGS: usize = 512;
const MAX_TOKENS: usize = 256;
const MAX_DEPTH: usize = 16;
const MAX_DATA_NODES: usize = 4096;
const MAX_DATA_BYTES: usize = 256 * 1024;
const MAX_LIST_ITEMS: usize = 128;
const MAX_ITERATIONS: usize = 4096;

#[derive(Default)]
pub(super) struct Work {
    rules: usize,
    hints: usize,
    evaluations: [usize; 3],
    tokens: [usize; 3],
}
#[derive(Clone, Copy)]
enum Block {
    If(bool),
    For(&'static str),
}
fn invalid(reason: &'static str) -> Error {
    Error::config(reason)
}

pub(super) fn validate(template: &str) -> Result<Work, Error> {
    let mut work = Work::default();
    let mut blocks = Vec::new();
    let mut rest = template;
    let mut tags = 0;
    while let Some(start) = rest.find('{') {
        rest = &rest[start..];
        let (opener, closer) = if rest.starts_with("{{") {
            ("{{", "}}")
        } else if rest.starts_with("{%") {
            ("{%", "%}")
        } else if rest.starts_with("{#") {
            ("{#", "#}")
        } else {
            rest = &rest[1..];
            continue;
        };
        tags += 1;
        if tags > MAX_TAGS {
            return Err(invalid("SYSTEM template exceeds 512 tags"));
        }
        let body = &rest[2..];
        let end = closing(body, closer, opener == "{#")?;
        let text = body[..end]
            .trim()
            .strip_prefix('-')
            .unwrap_or(body[..end].trim())
            .trim();
        let text = text.strip_suffix('-').unwrap_or(text).trim();
        rest = &body[end + 2..];
        if opener == "{#" {
            continue;
        }
        let alias = blocks.iter().find_map(|block| match block {
            Block::For(alias) => Some(*alias),
            _ => None,
        });
        let mut parser = Parser::new(text, alias)?;
        let scope = match alias {
            Some("rule") => 1,
            Some("hint") => 2,
            _ => 0,
        };
        work.evaluations[scope] += 1;
        work.tokens[scope] += parser.tokens.len().max(1);
        if opener == "{{" {
            parser.expression(0)?;
            parser.finished()?;
            continue;
        }
        let statement = parser.ident()?;
        match statement {
            "if" => {
                parser.condition = true;
                parser.expression(0)?;
                blocks.push(Block::If(false));
            }
            "elif" => {
                if !matches!(blocks.last(), Some(Block::If(false))) {
                    return Err(invalid("SYSTEM elif requires an open if before else"));
                }
                parser.condition = true;
                parser.expression(0)?;
            }
            "else" => match blocks.last_mut() {
                Some(Block::If(seen)) if !*seen => *seen = true,
                _ => {
                    return Err(invalid(
                        "SYSTEM else requires an open if and may occur only once",
                    ));
                }
            },
            "endif" => {
                if !matches!(blocks.pop(), Some(Block::If(_))) {
                    return Err(invalid("SYSTEM endif does not match an if"));
                }
            }
            "for" => {
                if alias.is_some() {
                    return Err(invalid("SYSTEM nested loops are not supported"));
                }
                let binding = parser.ident()?;
                parser.keyword("in")?;
                parser.keyword("agent")?;
                parser.symbol(".")?;
                match (binding, parser.ident()?) {
                    ("rule", "rules") => {
                        work.rules += 1;
                        blocks.push(Block::For("rule"));
                    }
                    ("hint", "hints") => {
                        work.hints += 1;
                        blocks.push(Block::For("hint"));
                    }
                    _ => {
                        return Err(invalid(
                            "SYSTEM loops support only rule in agent.rules or hint in agent.hints",
                        ));
                    }
                }
            }
            "endfor" => {
                if !matches!(blocks.pop(), Some(Block::For(_))) {
                    return Err(invalid("SYSTEM endfor does not match a for"));
                }
            }
            _ => {
                return Err(invalid(
                    "SYSTEM statements support only if/elif/else/endif and bounded Agent-list for/endfor",
                ));
            }
        }
        parser.finished()?;
        if blocks.len() > MAX_DEPTH {
            return Err(invalid("SYSTEM statement nesting exceeds 16"));
        }
    }
    if !blocks.is_empty() {
        return Err(invalid("SYSTEM contains an unclosed statement"));
    }
    Ok(work)
}

fn closing(text: &str, closer: &str, comment: bool) -> Result<usize, Error> {
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in text.char_indices() {
        if !comment {
            if escaped {
                escaped = false;
                continue;
            }
            if let Some(delimiter) = quote {
                if ch == '\\' {
                    escaped = true;
                } else if ch == delimiter {
                    quote = None;
                }
                continue;
            }
            if ch == '\'' || ch == '"' {
                quote = Some(ch);
                continue;
            }
        }
        if text[index..].starts_with(closer) {
            return Ok(index);
        }
    }
    Err(invalid("SYSTEM contains an unclosed template tag"))
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Token<'a> {
    Ident(&'a str),
    Literal,
    Symbol(&'a str),
}
struct Parser<'a> {
    tokens: Vec<Token<'a>>,
    position: usize,
    alias: Option<&'static str>,
    filters: usize,
    growth: usize,
    condition: bool,
}
impl<'a> Parser<'a> {
    fn new(text: &'a str, alias: Option<&'static str>) -> Result<Self, Error> {
        let mut tokens = Vec::new();
        let mut offset = 0;
        while offset < text.len() {
            let ch = text[offset..]
                .chars()
                .next()
                .expect("offset is inside text");
            if ch.is_whitespace() {
                offset += ch.len_utf8();
                continue;
            }
            let start = offset;
            if ch.is_ascii_alphabetic() || ch == '_' {
                offset += 1;
                while text
                    .as_bytes()
                    .get(offset)
                    .is_some_and(|c| c.is_ascii_alphanumeric() || *c == b'_')
                {
                    offset += 1;
                }
                tokens.push(Token::Ident(&text[start..offset]));
            } else if ch == '\'' || ch == '"' {
                offset += 1;
                let mut escaped = false;
                let mut closed = false;
                while offset < text.len() {
                    let next = text[offset..]
                        .chars()
                        .next()
                        .expect("offset is inside text");
                    offset += next.len_utf8();
                    if escaped {
                        escaped = false;
                    } else if next == '\\' {
                        escaped = true;
                    } else if next == ch {
                        closed = true;
                        break;
                    }
                }
                if !closed || offset - start > 4096 {
                    return Err(invalid(
                        "SYSTEM expression strings must be closed and at most 4096 bytes",
                    ));
                }
                tokens.push(Token::Literal);
            } else if ch.is_ascii_digit()
                || (ch == '-'
                    && text
                        .as_bytes()
                        .get(offset + 1)
                        .is_some_and(u8::is_ascii_digit))
            {
                offset += 1;
                while text
                    .as_bytes()
                    .get(offset)
                    .is_some_and(|c| c.is_ascii_digit() || matches!(*c, b'.' | b'e' | b'E'))
                {
                    offset += 1;
                }
                tokens.push(Token::Literal);
            } else {
                let two = text.get(offset..offset + 2);
                let len = if matches!(two, Some("==" | "!=" | "<=" | ">=")) {
                    2
                } else {
                    1
                };
                let symbol = text
                    .get(offset..offset + len)
                    .ok_or_else(|| invalid("SYSTEM expression contains an unsupported token"))?;
                if ![
                    ".", "|", "(", ")", "[", "]", ",", "=", "==", "!=", "<", ">", "<=", ">=",
                ]
                .contains(&symbol)
                {
                    return Err(invalid(
                        "SYSTEM expressions do not support arithmetic, functions or value-construction operators",
                    ));
                }
                offset += len;
                tokens.push(Token::Symbol(symbol));
            }
            if tokens.len() > MAX_TOKENS {
                return Err(invalid("SYSTEM tag exceeds 256 expression tokens"));
            }
        }
        Ok(Self {
            tokens,
            position: 0,
            alias,
            filters: 0,
            growth: 0,
            condition: false,
        })
    }
    fn peek(&self) -> Option<Token<'a>> {
        self.tokens.get(self.position).copied()
    }
    fn ident(&mut self) -> Result<&'a str, Error> {
        match self.peek() {
            Some(Token::Ident(name)) => {
                self.position += 1;
                Ok(name)
            }
            _ => Err(invalid("SYSTEM expression requires an identifier")),
        }
    }
    fn keyword(&mut self, word: &str) -> Result<(), Error> {
        if self.take(Token::Ident(word)) {
            Ok(())
        } else {
            Err(invalid(
                "SYSTEM statement has an unsupported binding or expression",
            ))
        }
    }
    fn symbol(&mut self, word: &str) -> Result<(), Error> {
        if self.take(Token::Symbol(word)) {
            Ok(())
        } else {
            Err(invalid(
                "SYSTEM expression has an unsupported or missing delimiter",
            ))
        }
    }
    fn take(&mut self, token: Token<'_>) -> bool {
        if self.peek().is_some_and(|next| next == token) {
            self.position += 1;
            true
        } else {
            false
        }
    }
    fn finished(&self) -> Result<(), Error> {
        if self.position == self.tokens.len() {
            Ok(())
        } else {
            Err(invalid(
                "SYSTEM expression contains unsupported trailing syntax",
            ))
        }
    }
    fn expression(&mut self, depth: usize) -> Result<(), Error> {
        if depth > MAX_DEPTH {
            return Err(invalid("SYSTEM expression nesting exceeds 16"));
        }
        self.conjunction(depth)?;
        while self.take(Token::Ident("or")) {
            self.conjunction(depth)?;
        }
        Ok(())
    }
    fn conjunction(&mut self, depth: usize) -> Result<(), Error> {
        self.comparison(depth)?;
        while self.take(Token::Ident("and")) {
            self.comparison(depth)?;
        }
        Ok(())
    }
    fn comparison(&mut self, depth: usize) -> Result<(), Error> {
        let mut not_count = 0;
        while self.take(Token::Ident("not")) {
            not_count += 1;
        }
        if not_count > MAX_DEPTH {
            return Err(invalid("SYSTEM boolean nesting exceeds 16"));
        }
        self.filtered(depth)?;
        if self.take(Token::Ident("is")) {
            self.take(Token::Ident("not"));
            self.keyword("defined")?;
        } else if matches!(
            self.peek(),
            Some(Token::Symbol("==" | "!=" | "<" | ">" | "<=" | ">="))
        ) {
            self.position += 1;
            self.filtered(depth)?;
        }
        Ok(())
    }
    fn filtered(&mut self, depth: usize) -> Result<(), Error> {
        self.primary(depth)?;
        while self.take(Token::Symbol("|")) {
            self.filters += 1;
            if self.filters > 8 {
                return Err(invalid("SYSTEM tag exceeds eight filters"));
            }
            let filter = self.ident()?;
            if ![
                "safe",
                "length",
                "default",
                "json_encode",
                "upper",
                "lower",
                "trim",
                "capitalize",
                "title",
                "str",
                "int",
                "float",
                "first",
                "last",
            ]
            .contains(&filter)
            {
                return Err(invalid(
                    "SYSTEM filter is outside the bounded supported filter set",
                ));
            }
            if [
                "json_encode",
                "upper",
                "lower",
                "capitalize",
                "title",
                "str",
            ]
            .contains(&filter)
            {
                if self.condition {
                    return Err(invalid(
                        "SYSTEM text-expanding filters are supported only in output expressions",
                    ));
                }
                self.growth += 1;
                if self.growth > 1 {
                    return Err(invalid(
                        "SYSTEM tag allows at most one text-expanding filter",
                    ));
                }
            }
            if self.take(Token::Symbol("(")) {
                if filter != "default" && filter != "json_encode" {
                    return Err(invalid(
                        "SYSTEM only default and json_encode accept filter arguments",
                    ));
                }
                let name = self.ident()?;
                if (filter == "default" && name != "value")
                    || (filter == "json_encode" && name != "pretty")
                {
                    return Err(invalid("SYSTEM filter argument is unsupported"));
                }
                self.symbol("=")?;
                self.literal()?;
                self.symbol(")")?;
            }
        }
        Ok(())
    }
    fn literal(&mut self) -> Result<(), Error> {
        match self.peek() {
            Some(Token::Literal | Token::Ident("true" | "false" | "none" | "null")) => {
                self.position += 1;
                Ok(())
            }
            _ => Err(invalid(
                "SYSTEM filter arguments and indices must be literal values",
            )),
        }
    }
    fn primary(&mut self, depth: usize) -> Result<(), Error> {
        if self.take(Token::Symbol("(")) {
            self.expression(depth + 1)?;
            return self.symbol(")");
        }
        if matches!(
            self.peek(),
            Some(Token::Literal | Token::Ident("true" | "false" | "none" | "null"))
        ) {
            return self.literal();
        }
        let name = self.ident()?;
        if name != "agent" && name != "client_prompt" && self.alias != Some(name) {
            return Err(invalid(
                "SYSTEM values must come from agent, client_prompt or the current declared loop binding",
            ));
        }
        let mut segments = 0;
        loop {
            if self.take(Token::Symbol(".")) {
                self.ident()?;
            } else if self.take(Token::Symbol("[")) {
                self.literal()?;
                self.symbol("]")?;
            } else {
                break;
            }
            segments += 1;
            if segments > MAX_DEPTH {
                return Err(invalid("SYSTEM data path exceeds 16 segments"));
            }
        }
        Ok(())
    }
}

impl Work {
    pub(super) fn validate_data(
        &self,
        agent: Option<&microcrab_agent_types::AgentInfo>,
        client_prompt: Option<&str>,
    ) -> Result<(), Error> {
        let mut nodes = 1;
        let mut raw_bytes = 0usize;
        fn walk(
            value: &Value,
            depth: usize,
            nodes: &mut usize,
            raw_bytes: &mut usize,
        ) -> Result<(), Error> {
            *nodes += 1;
            if depth > MAX_DEPTH || *nodes > MAX_DATA_NODES {
                return Err(invalid("SYSTEM Agent data exceeds depth or node budget"));
            }
            match value {
                Value::String(text) => *raw_bytes = raw_bytes.saturating_add(text.len()),
                Value::Array(items) => {
                    for item in items {
                        walk(item, depth + 1, nodes, raw_bytes)?;
                    }
                }
                Value::Object(items) => {
                    for (key, item) in items {
                        *raw_bytes = raw_bytes.saturating_add(key.len());
                        walk(item, depth + 1, nodes, raw_bytes)?;
                    }
                }
                _ => (),
            }
            if *raw_bytes > MAX_DATA_BYTES {
                return Err(invalid("SYSTEM input data exceeds 256 KiB"));
            }
            Ok(())
        }
        struct Count(usize);
        impl std::io::Write for Count {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self.0.saturating_add(bytes.len());
                if self.0 > MAX_DATA_BYTES {
                    return Err(std::io::Error::other("SYSTEM input data exceeds 256 KiB"));
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut encoded = Count(0);
        if let Some(agent) = agent {
            for (key, value) in agent.variables.values() {
                raw_bytes = raw_bytes.saturating_add(key.len());
                walk(value, 1, &mut nodes, &mut raw_bytes)?;
            }
            serde_json::to_writer(&mut encoded, &agent.variables)
                .map_err(|_| invalid("SYSTEM input data exceeds 256 KiB"))?;
        }
        let client_bytes = client_prompt.map_or(0, str::len);
        if client_bytes > MAX_DATA_BYTES {
            return Err(invalid("SYSTEM input data exceeds 256 KiB"));
        }
        let length = |key: &str, used: usize| -> Result<usize, Error> {
            match agent.and_then(|agent| agent.variables.get(key)) {
                Some(Value::Array(items)) => Ok(items.len()),
                Some(_) if used > 0 => Err(invalid(
                    "SYSTEM loop sources agent.rules and agent.hints must be arrays",
                )),
                _ => Ok(0),
            }
        };
        let rules = length("rules", self.rules)?;
        let hints = length("hints", self.hints)?;
        if rules > MAX_LIST_ITEMS
            || hints > MAX_LIST_ITEMS
            || self.rules * rules + self.hints * hints > MAX_ITERATIONS
        {
            return Err(invalid(
                "SYSTEM Agent-list loops exceed 128 items or 4096 aggregate iterations",
            ));
        }
        let evaluations =
            self.evaluations[0] + self.evaluations[1] * rules + self.evaluations[2] * hints;
        let token_work = self.tokens[0] + self.tokens[1] * rules + self.tokens[2] * hints;
        if evaluations > 4096
            || token_work > 65536
            || evaluations.saturating_mul(encoded.0 + client_bytes) > 128 * 1024 * 1024
        {
            return Err(invalid(
                "SYSTEM evaluation exceeds 4096 expanded tags, 65536 token-work or 128 MiB data-work",
            ));
        }
        Ok(())
    }
}
