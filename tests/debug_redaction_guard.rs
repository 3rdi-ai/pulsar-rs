//! Source-scan guard: fails if any struct or enum under `src/` that holds a
//! credential derives `Debug`.
//!
//! In this fork, OAuth2 client secrets, `data:` URLs, Basic `auth_data`, and
//! raw `CommandConnect` messages leaked through derived `Debug` twice. A
//! derived `Debug` prints every field verbatim, so any type that stores a
//! credential must implement `Debug` manually and redact the secret (see
//! `src/authentication/basic.rs`).
//!
//! The scan parses each file with `syn`, so commented-out derives never count
//! and an unparseable file fails the test (fail closed). An exemption requires
//! an entry in [`EXEMPT`] with a non-empty reason; a stale entry also fails.
//!
//! Blind spots:
//! (a) a hand-written `impl Debug` that prints a secret is not inspected (review covers it; the behavioural tests in src/authentication/oauth2.rs cover the known types),
//! (b) a generic field `T` instantiated with a secret type is not resolved,
//! (c) types defined inside macro bodies are invisible, which `no_macro_in_src_defines_a_type` keeps fail-closed; (d) `use … as` and `type` aliases resolve only
//! from a file's top-level items, within that file,
//! (e) aliases declared in another file are not resolved, and
//! (f) a field whose type is an innocently named alias (e.g. `type T = String`) inside a type whose name and field names carry no secret word is not flagged.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::{Path, PathBuf};

use proc_macro2::{TokenStream, TokenTree};
use syn::visit::Visit;

/// A lowercased field name containing any of these words is secret-bearing.
const SECRET_FIELD_WORDS: &[&str] = &[
    "secret",
    "token",
    "password",
    "credential",
    "private_key",
    "auth_data",
    "jwt",
    "bearer",
    "api_key",
    "passphrase",
];

/// A field whose type path has a segment with exactly one of these idents is
/// secret-bearing: raw proto commands carry `auth_data`.
const SECRET_TYPES: &[&str] = &[
    "BaseCommand",
    "CommandConnect",
    "AuthData",
    "CommandAuthResponse",
    "CommandAuthChallenge",
    "Authentication",
    "CachedToken",
];

/// A type (or enum variant) whose lowercased ident contains one of these is
/// secret-bearing whatever its fields are called.
const SECRET_TYPE_NAME_WORDS: &[&str] = &["auth", "token", "credential", "secret"];

/// Exemptions as `(trigger key, reason)`. An entry covers ONE trigger (a field, a variant, or the type's own name);
/// a new trigger in an exempt type fails, and a stale entry fails.
/// The key is `"<path relative to crate root with forward slashes>::<TypeName>[::<Trigger>]"`.
const EXEMPT: &[(&str, &str)] = &[
    (
        "src/connection.rs::RequestKey::AuthChallenge",
        "a unit variant: a request-routing tag that carries no data",
    ),
    (
        "src/error.rs::AuthenticationError",
        "flagged by its name; holds only error message strings, built kind-only at their source \
         (src/authentication/oauth2.rs tests assert no secret reaches them)",
    ),
    (
        "src/error.rs::ConnectionError::Authentication",
        "wraps AuthenticationError: see that entry",
    ),
    (
        "src/error.rs::Error::Authentication",
        "wraps AuthenticationError: see that entry",
    ),
];

struct Aliases {
    secret_types: Vec<String>,
    debug_names: Vec<String>,
}

fn collect_aliases(file: &syn::File) -> Aliases {
    let mut aliases = Aliases {
        secret_types: Vec::new(),
        debug_names: vec!["Debug".to_string()],
    };

    // First pass: collect use renames
    for item in &file.items {
        if let syn::Item::Use(item_use) = item {
            collect_use_aliases(&item_use.tree, &mut aliases);
        }
    }

    // Second pass: `type` aliases over a secret type (or an earlier alias)
    for item in &file.items {
        if let syn::Item::Type(item_type) = item {
            let mut visitor = TypeSecretVisitor {
                found: false,
                aliases: &aliases,
            };
            visitor.visit_type(&item_type.ty);
            if visitor.found {
                aliases.secret_types.push(item_type.ident.to_string());
            }
        }
    }

    aliases
}

fn collect_use_aliases(tree: &syn::UseTree, aliases: &mut Aliases) {
    match tree {
        syn::UseTree::Path(p) => collect_use_aliases(&p.tree, aliases),
        syn::UseTree::Group(g) => {
            for item in &g.items {
                collect_use_aliases(item, aliases);
            }
        }
        // an un-renamed import keeps its ident, which the scan already matches
        syn::UseTree::Name(_) => {}
        syn::UseTree::Rename(r) => {
            let ident_str = r.ident.to_string();
            if SECRET_TYPES.contains(&ident_str.as_str())
                || aliases.secret_types.contains(&ident_str)
            {
                aliases.secret_types.push(r.rename.to_string());
            }
            if r.ident == "Debug" {
                aliases.debug_names.push(r.rename.to_string());
            }
        }
        syn::UseTree::Glob(_) => {}
    }
}

/// True if any `derive` attribute lists a path whose last segment is `Debug`,
/// or any `cfg_attr` attribute contains a `derive(...)` that includes `Debug`.
///
/// Fails closed: a `parse_nested_meta` error panics.
fn derives_debug(attrs: &[syn::Attribute], aliases: &Aliases) -> bool {
    for attr in attrs {
        if attr.path().is_ident("derive") {
            let mut found = false;
            let result = attr.parse_nested_meta(|meta| {
                if let Some(seg) = meta.path.segments.last() {
                    if aliases
                        .debug_names
                        .iter()
                        .any(|d| d == &seg.ident.to_string())
                    {
                        found = true;
                    }
                }
                Ok(())
            });
            if result.is_err() {
                panic!("failed to parse derive attribute; the guard fails closed");
            }
            if found {
                return true;
            }
        } else if attr.path().is_ident("cfg_attr") {
            let mut found = false;
            let result = attr.parse_nested_meta(|meta| cfg_attr_item(meta, aliases, &mut found));
            if result.is_err() {
                panic!("failed to parse cfg_attr attribute; the guard fails closed");
            }
            if found {
                return true;
            }
        }
    }
    false
}

/// One item of a `cfg_attr(...)` list: a `derive(...)` sets `found` when it
/// names a Debug derive, a nested `cfg_attr(...)` recurses, and a cfg
/// predicate (`test`, `feature = "x"`, `all(...)`) is skipped.
fn cfg_attr_item(
    meta: syn::meta::ParseNestedMeta<'_>,
    aliases: &Aliases,
    found: &mut bool,
) -> syn::Result<()> {
    if meta.path.is_ident("derive") {
        meta.parse_nested_meta(|d| {
            if d.path
                .segments
                .last()
                .is_some_and(|s| aliases.debug_names.iter().any(|n| s.ident == n.as_str()))
            {
                *found = true;
            }
            Ok(())
        })
    } else if meta.path.is_ident("cfg_attr") {
        meta.parse_nested_meta(|inner| cfg_attr_item(inner, aliases, found))
    } else if meta.input.peek(syn::Token![=]) {
        meta.value()?.parse::<syn::Expr>()?;
        Ok(())
    } else if meta.input.peek(syn::token::Paren) {
        meta.input.parse::<proc_macro2::Group>()?;
        Ok(())
    } else {
        Ok(())
    }
}

/// True if the field name contains a secret word or its type references a
/// secret-bearing type.
fn field_is_secret(field: &syn::Field, aliases: &Aliases) -> bool {
    if let Some(ident) = &field.ident {
        let name = ident.to_string().to_lowercase();
        if SECRET_FIELD_WORDS.iter().any(|w| name.contains(w)) {
            return true;
        }
    }

    let mut visitor = TypeSecretVisitor {
        found: false,
        aliases,
    };
    visitor.visit_type(&field.ty);
    visitor.found
}

struct TypeSecretVisitor<'a> {
    found: bool,
    aliases: &'a Aliases,
}

impl<'ast, 'a> Visit<'ast> for TypeSecretVisitor<'a> {
    fn visit_path_segment(&mut self, i: &'ast syn::PathSegment) {
        let ident_str = i.ident.to_string();
        if SECRET_TYPES.contains(&ident_str.as_str())
            || self.aliases.secret_types.contains(&ident_str)
        {
            self.found = true;
        }
        syn::visit::visit_path_segment(self, i);
    }
}

/// True if any field in the given fields is secret-bearing.
fn fields_secret(fields: &syn::Fields, aliases: &Aliases) -> bool {
    fields.iter().any(|f| field_is_secret(f, aliases))
}

fn ident_is_secret(ident: &syn::Ident) -> bool {
    let name = ident.to_string().to_lowercase();
    SECRET_TYPE_NAME_WORDS.iter().any(|w| name.contains(w))
}

/// Collects flagged types during a `syn` visit.
struct Scan {
    file: String,
    flagged: Vec<String>,
    aliases: Aliases,
}

impl<'ast> Visit<'ast> for Scan {
    fn visit_item_struct(&mut self, i: &'ast syn::ItemStruct) {
        if derives_debug(&i.attrs, &self.aliases) {
            if ident_is_secret(&i.ident) {
                self.flagged.push(format!("{}::{}", self.file, i.ident));
            }
            for (idx, f) in i.fields.iter().enumerate() {
                if field_is_secret(f, &self.aliases) {
                    let name = f
                        .ident
                        .as_ref()
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| idx.to_string());
                    self.flagged
                        .push(format!("{}::{}::{}", self.file, i.ident, name));
                }
            }
        }
        syn::visit::visit_item_struct(self, i);
    }

    fn visit_item_enum(&mut self, i: &'ast syn::ItemEnum) {
        if derives_debug(&i.attrs, &self.aliases) {
            if ident_is_secret(&i.ident) {
                self.flagged.push(format!("{}::{}", self.file, i.ident));
            }
            for v in &i.variants {
                let v_name = v.ident.to_string().to_lowercase();
                let is_secret = fields_secret(&v.fields, &self.aliases)
                    || SECRET_TYPE_NAME_WORDS.iter().any(|w| v_name.contains(w))
                    || SECRET_FIELD_WORDS.iter().any(|w| v_name.contains(w));
                if is_secret {
                    self.flagged
                        .push(format!("{}::{}::{}", self.file, i.ident, v.ident));
                }
            }
        }
        syn::visit::visit_item_enum(self, i);
    }
}

/// Parses `src` and returns the list of flagged type keys.
fn flag_source(file: &str, src: &str) -> Vec<String> {
    let file_ast = syn::parse_file(src)
        .unwrap_or_else(|e| panic!("{file}: unparseable, the guard fails closed: {e}"));
    let aliases = collect_aliases(&file_ast);
    let mut scan = Scan {
        file: file.to_string(),
        flagged: Vec::new(),
        aliases,
    };
    scan.visit_file(&file_ast);
    scan.flagged
}

/// Recursively collects `.rs` files under `dir`.
fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("failed to read directory {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("failed to read dir entry: {e}"));
        let path = entry.path();
        if path.is_dir() {
            rs_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn src_sources() -> Vec<(String, String)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rs_files(&root.join("src"), &mut files);

    let mut sources = Vec::new();
    for path in &files {
        let rel = path
            .strip_prefix(root)
            .unwrap_or_else(|e| panic!("failed to strip prefix: {e}"))
            .to_string_lossy()
            .replace('\\', "/");
        let src = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        sources.push((rel, src));
    }
    sources
}

#[test]
fn no_credential_type_derives_debug() {
    let sources = src_sources();

    // Check for specific files to ensure enumeration works
    let rel_paths: Vec<&str> = sources.iter().map(|(p, _)| p.as_str()).collect();
    for name in &[
        "src/authentication/oauth2.rs",
        "src/authentication/basic.rs",
        "src/connection.rs",
    ] {
        assert!(
            rel_paths.contains(name),
            "scan missed {name}: the src/ enumeration is broken"
        );
    }

    let mut flagged = Vec::new();
    for (rel, src) in &sources {
        flagged.extend(flag_source(rel, src));
    }
    flagged.sort();

    let exempt: Vec<(&str, &str)> = EXEMPT.iter().map(|(k, r)| (*k, *r)).collect();
    let (unexempt, stale) = partition(&flagged, &exempt);

    assert!(
        unexempt.is_empty(),
        "these types derive Debug while holding a credential; write a manual Debug that prints <redacted> (see src/authentication/basic.rs), or add an EXEMPT entry with a reason: {unexempt:#?}"
    );
    assert!(
        stale.is_empty(),
        "stale EXEMPT entries (no longer flagged): {stale:#?}"
    );
    for (_, reason) in EXEMPT {
        assert!(!reason.is_empty(), "EXEMPT reason must be non-empty");
    }
}

fn partition<'a>(
    flagged: &'a [String],
    exempt: &[(&'a str, &str)],
) -> (Vec<&'a str>, Vec<&'a str>) {
    let exempt_keys: Vec<&str> = exempt.iter().map(|(k, _)| *k).collect();
    let unexempt: Vec<&'a str> = flagged
        .iter()
        .map(|s| s.as_str())
        .filter(|f| !exempt_keys.contains(f))
        .collect();
    let stale: Vec<&'a str> = exempt_keys
        .iter()
        .filter(|k| !flagged.iter().any(|f| f == *k))
        .copied()
        .collect();
    (unexempt, stale)
}

#[test]
fn classifier_fixtures() {
    assert_eq!(
        flag_source(
            "f.rs",
            "#[derive(Debug, Clone)] struct A { password: String }"
        ),
        vec!["f.rs::A::password"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "#[derive(Clone)] struct A { password: String } impl std::fmt::Debug for A { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { Ok(()) } }"
        ),
        Vec::<String>::new()
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "#[cfg_attr(test, derive(Debug))] struct A { client_secret: String }"
        ),
        vec!["f.rs::A::client_secret"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "// #[derive(Debug)] struct A { token: String }\n#[derive(Debug)] struct B { id: u32 }"
        ),
        Vec::<String>::new()
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "#[derive(Debug)] enum E { Sent(proto::BaseCommand) }"
        ),
        vec!["f.rs::E::Sent"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "mod m { #[derive(std::fmt::Debug)] pub struct Inner { auth_data: Vec<u8> } }"
        ),
        vec!["f.rs::Inner::auth_data"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "#[derive(Debug)] struct A { data: Option<AuthDataLike> }"
        ),
        Vec::<String>::new()
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "#[derive(Debug)] struct Authentication { name: String, data: Vec<u8> }"
        ),
        vec!["f.rs::Authentication"]
    );

    assert_eq!(
        flag_source("f.rs", "#[derive(Debug)] struct Token(Vec<u8>);"),
        vec!["f.rs::Token"]
    );

    assert_eq!(
        flag_source("f.rs", "#[derive(Debug)] enum Cred { Jwt(String) }"),
        vec!["f.rs::Cred::Jwt"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "use std::fmt::Debug as D; #[derive(D)] struct S { password: String }"
        ),
        vec!["f.rs::S::password"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "use crate::connection::Authentication as A; #[derive(Debug)] struct W { a: A }"
        ),
        vec!["f.rs::W::a"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "type Raw = proto::CommandConnect; #[derive(Debug)] struct W { c: Raw }"
        ),
        vec!["f.rs::W::c"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "#[derive(Debug)] struct S { bearer: String, api_key: String }"
        ),
        vec!["f.rs::S::bearer", "f.rs::S::api_key"]
    );

    assert_eq!(
        flag_source("f.rs", "#[derive(Debug)] struct S { key: u32 }"),
        Vec::<String>::new()
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "#[derive(Debug)] struct W { c: Option<Box<CachedToken>> }"
        ),
        vec!["f.rs::W::c"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "#[cfg_attr(all(test, feature = \"x\"), derive(Clone, Debug))] struct A { token: u8 }"
        ),
        vec!["f.rs::A::token"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "#[cfg_attr(test, derive(Clone))] struct A { token: u8 }"
        ),
        Vec::<String>::new()
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "#[cfg_attr(feature = \"x\", cfg_attr(test, derive(Debug)))] struct S { token: String }"
        ),
        vec!["f.rs::S::token"]
    );

    assert_eq!(
        flag_source("f.rs", "#[derive(Debug)] struct P(u8, Vec<CachedToken>);"),
        vec!["f.rs::P::1"]
    );
}

#[test]
fn an_exempt_type_with_a_new_trigger_still_fails() {
    let flagged = flag_source(
        "f.rs",
        "#[derive(Debug)] enum RequestKey { AuthChallenge, Token(Vec<u8>) }",
    );
    assert_eq!(
        flagged,
        vec!["f.rs::RequestKey::AuthChallenge", "f.rs::RequestKey::Token"]
    );
    let (unexempt, stale) = partition(&flagged, &[("f.rs::RequestKey::AuthChallenge", "unit tag")]);
    assert_eq!(unexempt, vec!["f.rs::RequestKey::Token"]);
    assert!(stale.is_empty());
    let (_, stale) = partition(&flagged, &[("f.rs::RequestKey::Gone", "stale")]);
    assert_eq!(stale, vec!["f.rs::RequestKey::Gone"]);
}

/// True if `struct` or `enum` appears as an IDENT at any depth (a literal
/// such as a log message never counts).
fn macro_defines_type(tokens: &TokenStream) -> bool {
    tokens.clone().into_iter().any(|tt| match tt {
        TokenTree::Ident(ident) => ident == "struct" || ident == "enum",
        TokenTree::Group(g) => macro_defines_type(&g.stream()),
        _ => false,
    })
}

struct MacroScan {
    file: String,
    flagged: Vec<String>,
}

impl<'ast> Visit<'ast> for MacroScan {
    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        if macro_defines_type(&m.tokens) {
            let name = m
                .path
                .segments
                .last()
                .map(|s| s.ident.to_string())
                .unwrap_or_default();
            self.flagged.push(format!("{}::{}!", self.file, name));
        }
        syn::visit::visit_macro(self, m);
    }
}

fn macro_types(file: &str, src: &str) -> Vec<String> {
    let file_ast = syn::parse_file(src)
        .unwrap_or_else(|e| panic!("{file}: unparseable, the guard fails closed: {e}"));
    let mut scan = MacroScan {
        file: file.to_string(),
        flagged: Vec::new(),
    };
    scan.visit_file(&file_ast);
    scan.flagged
}

#[test]
fn no_macro_in_src_defines_a_type() {
    let sources = src_sources();
    let mut found = Vec::new();
    for (rel, src) in &sources {
        found.extend(macro_types(rel, src));
    }
    assert!(
        found.is_empty(),
        "a macro in src/ defines a struct or enum; the Debug scan cannot see inside macro bodies, so define the type outside the macro: {found:#?}"
    );
}

#[test]
fn macro_fixtures() {
    assert_eq!(
        macro_types("f.rs", "macro_rules! m { () => { struct X; } }"),
        vec!["f.rs::macro_rules!"]
    );
    assert_eq!(
        macro_types("f.rs", "fn f() { log::debug!(\"a struct and an enum\"); }"),
        Vec::<String>::new()
    );
    assert_eq!(macro_types("f.rs", "m! { enum E { A } }"), vec!["f.rs::m!"]);
}

#[test]
#[should_panic(expected = "cfg_attr")]
fn unparseable_cfg_attr_derive_fails_closed() {
    flag_source(
        "f.rs",
        "#[cfg_attr(test, derive(Debug Clone))] struct A { id: u8 }",
    );
}

#[test]
#[should_panic(expected = "unparseable")]
fn unparseable_source_fails_closed() {
    flag_source("f.rs", "struct {");
}
