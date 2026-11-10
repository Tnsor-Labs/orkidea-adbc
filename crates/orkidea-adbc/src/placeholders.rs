// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The bind placeholders of a statement, and each use of them.
//!
//! In SQL, every use of a placeholder is a bind of its own, so
//! `WHERE a = :v OR b = :v` takes two values; in PL/SQL a name binds once.
//! oracledb lists each name once (`Statement::bind_names`), which is enough
//! to bind by position but not to bind a repeated name by name. The uses
//! come from a port of oracledb's client-side SQL parser, and are checked
//! against its names.

use std::collections::HashSet;

/// The placeholders of one statement.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Placeholders {
    /// Each name once, in order of first appearance, as oracledb names them:
    /// unquoted names upper-cased, quoted names without their quotes.
    pub names: Vec<String>,
    /// The name of each bind the statement takes, in order: one per use in
    /// SQL, one per name in PL/SQL.
    pub uses: Vec<String>,
}

impl Placeholders {
    /// The placeholders of a statement oracledb has parsed (client-side).
    pub(crate) fn of(statement: &oracledb::Statement) -> Self {
        let names = statement.bind_names().to_vec();
        let uses = if statement.is_plsql() {
            names.clone()
        } else {
            let uses = placeholder_uses(statement.sql());
            // Should the two parsers ever disagree, bind each name once, as
            // oracledb does for a positional bind of its names.
            if first_appearances(&uses) == names {
                uses
            } else {
                names.clone()
            }
        };
        Self { names, uses }
    }
}

fn first_appearances(uses: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    uses.iter()
        .filter(|name| seen.insert(name.as_str()))
        .cloned()
        .collect()
}

/// Each use of a bind placeholder in `sql`, in order. This follows oracledb's
/// `SqlParser::parse` (`statement/sql_parser.rs`) character for character,
/// so that it finds exactly the binds oracledb sends: placeholders in string
/// literals, q-strings, quoted identifiers, and comments are not binds, nor
/// is a colon right after a string (JSON constant syntax).
fn placeholder_uses(sql: &str) -> Vec<String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut uses = Vec::new();
    let mut pos = 0;
    let mut last_ch = ' ';
    let mut last_was_string = false;
    while let Some(&ch) = chars.get(pos) {
        if ch == '\'' {
            last_was_string = true;
            pos = if last_ch.eq_ignore_ascii_case(&'q') {
                skip_qstring(&chars, pos)
            } else {
                skip_quoted(&chars, pos)
            };
        } else if !ch.is_whitespace() {
            if ch == '-' && last_ch == '-' {
                while chars.get(pos).is_some_and(|&c| c != '\n') {
                    pos += 1;
                }
            } else if ch == '*' && last_ch == '/' {
                pos = skip_block_comment(&chars, pos);
            } else if ch == '"' {
                pos = skip_quoted(&chars, pos);
            } else if ch == ':' && !last_was_string {
                pos = bind_name(&chars, pos, &mut uses);
            }
            last_was_string = false;
        }
        pos += 1;
        last_ch = ch;
    }
    uses
}

/// From the opening quote at `pos`, the position of the closing quote (or
/// the end of the SQL).
fn skip_quoted(chars: &[char], pos: usize) -> usize {
    let quote = chars[pos];
    let mut pos = pos + 1;
    while chars.get(pos).is_some_and(|&c| c != quote) {
        pos += 1;
    }
    pos
}

/// From the quote after `q` at `pos`, the position of the quote that ends
/// the q-string: `q'[...]'`, `q'{...}'`, `q'<...>'`, `q'(...)'`, or `q'?...?'`.
fn skip_qstring(chars: &[char], pos: usize) -> usize {
    let Some(&start) = chars.get(pos + 1) else {
        return chars.len();
    };
    let end = match start {
        '[' => ']',
        '{' => '}',
        '<' => '>',
        '(' => ')',
        other => other,
    };
    let mut pos = pos + 2;
    while let Some(&ch) = chars.get(pos) {
        pos += 1;
        if ch == end && chars.get(pos) == Some(&'\'') {
            return pos;
        }
    }
    chars.len()
}

/// From the `*` of `/*` at `pos`, the position of the `/` that ends the
/// comment.
fn skip_block_comment(chars: &[char], pos: usize) -> usize {
    let mut last_ch = ' ';
    let mut pos = pos + 1;
    while let Some(&ch) = chars.get(pos) {
        if ch == '/' && last_ch == '*' {
            return pos;
        }
        pos += 1;
        last_ch = ch;
    }
    chars.len()
}

/// Reads the bind name after the colon at `start`, if any, into `uses`.
/// Returns the position the parser continues from: the character that ended
/// the name, or the colon when no name follows it.
fn bind_name(chars: &[char], start: usize, uses: &mut Vec<String>) -> usize {
    let mut quoted = false;
    let mut digits_only = false;
    let mut in_bind = false;
    let mut pos = start + 1;
    while let Some(&ch) = chars.get(pos) {
        if !in_bind {
            if ch.is_whitespace() {
                pos += 1;
                continue;
            } else if ch == '"' {
                quoted = true;
            } else if ch.is_ascii_digit() {
                digits_only = true;
            } else if !ch.is_alphabetic() {
                return start;
            }
            in_bind = true;
        } else if (digits_only && !ch.is_ascii_digit())
            || (quoted && ch == '"')
            || (!digits_only
                && !quoted
                && !ch.is_alphanumeric()
                && ch != '_'
                && ch != '$'
                && ch != '#')
        {
            break;
        }
        pos += 1;
    }
    if in_bind {
        let text: String = chars[start + 1..pos].iter().collect();
        let text = text.trim_start();
        uses.push(if quoted {
            text[1..].to_string()
        } else if digits_only {
            text.to_string()
        } else {
            text.to_uppercase()
        });
    }
    pos
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uses(sql: &str) -> Vec<String> {
        placeholder_uses(sql)
    }

    #[test]
    fn lists_every_use_in_order() {
        assert_eq!(
            uses("select :a, :b from t where x = :a or y = :B"),
            ["A", "B", "A", "B"]
        );
        assert_eq!(
            uses("select * from t where id in (:1, :2, :1)"),
            ["1", "2", "1"]
        );
    }

    #[test]
    fn reads_names_as_oracledb_does() {
        assert_eq!(
            uses(r#"select : spaced, :"Mixed Case", :x$y#z_1, :12 from dual"#),
            ["SPACED", "Mixed Case", "X$Y#Z_1", "12"]
        );
    }

    #[test]
    fn skips_literals_identifiers_and_comments() {
        let sql = r#"select ':no', q'[:no]', Q'{it's :no}', "a:no", :yes -- :no
            /* :no */ from t where t.c = :yes2 and j = '{"a"}':json"#;
        assert_eq!(uses(sql), ["YES", "YES2"]);
        assert_eq!(uses("select 'it''s :no' from dual where a = :v"), ["V"]);
    }

    #[test]
    fn ignores_colons_that_start_no_name() {
        assert_eq!(uses("select :=, : , :+1 from dual"), Vec::<String>::new());
    }

    #[test]
    fn unterminated_text_ends_the_scan() {
        assert_eq!(uses("select :a, 'open"), ["A"]);
        assert_eq!(uses("select :a /* open"), ["A"]);
        assert_eq!(uses("select :a, q'["), ["A"]);
    }

    #[test]
    fn first_appearances_keep_order() {
        let names = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(
            first_appearances(&names(&["B", "A", "B", "C", "A"])),
            names(&["B", "A", "C"])
        );
    }
}
