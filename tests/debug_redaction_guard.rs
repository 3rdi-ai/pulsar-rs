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

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::{Path, PathBuf};

use syn::visit::Visit;

/// A lowercased field name containing any of these words is secret-bearing.
const SECRET_FIELD_WORDS: &[&str] = &[
    "secret",
    "token",
    "password",
    "credential",
    "private_key",
    "auth_data",
];

/// A field whose type path has a segment with exactly one of these idents is
/// secret-bearing: raw proto commands carry `auth_data`.
const SECRET_TYPES: &[&str] = &[
    "BaseCommand",
    "CommandConnect",
    "AuthData",
    "CommandAuthResponse",
    "CommandAuthChallenge",
];

/// Exemptions as `(type key, reason)`. The key is
/// `"<path relative to crate root with forward slashes>::<TypeName>"`.
/// A stale entry (no longer flagged) fails the test.
const EXEMPT: &[(&str, &str)] = &[];

/// True if any `derive` attribute lists a path whose last segment is `Debug`,
/// or any `cfg_attr` attribute contains a `derive(...)` that includes `Debug`.
///
/// Fails closed: a `parse_nested_meta` error panics.
fn derives_debug(attrs: &[syn::Attribute]) -> bool {
    for attr in attrs {
        if attr.path().is_ident("derive") {
            let mut found = false;
            let result = attr.parse_nested_meta(|meta| {
                if meta
                    .path
                    .segments
                    .last()
                    .is_some_and(|seg| seg.ident == "Debug")
                {
                    found = true;
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
            let s = match &attr.meta {
                syn::Meta::List(l) => l.tokens.to_string(),
                _ => String::new(),
            };
            let compact = s.replace(' ', "");
            if compact.contains("derive(") {
                if let Some(idx) = compact.find("derive(") {
                    let after = &compact[idx + "derive(".len()..];
                    if after.contains("Debug") {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// True if the field name contains a secret word or its type references a
/// secret-bearing type.
fn field_is_secret(field: &syn::Field) -> bool {
    if let Some(ident) = &field.ident {
        let name = ident.to_string().to_lowercase();
        if SECRET_FIELD_WORDS.iter().any(|w| name.contains(w)) {
            return true;
        }
    }

    let mut visitor = TypeSecretVisitor { found: false };
    visitor.visit_type(&field.ty);
    visitor.found
}

struct TypeSecretVisitor {
    found: bool,
}

impl<'ast> Visit<'ast> for TypeSecretVisitor {
    fn visit_path_segment(&mut self, i: &'ast syn::PathSegment) {
        if SECRET_TYPES.iter().any(|t| i.ident == *t) {
            self.found = true;
        }
        syn::visit::visit_path_segment(self, i);
    }
}

/// True if any field in the given fields is secret-bearing.
fn fields_secret(fields: &syn::Fields) -> bool {
    fields.iter().any(field_is_secret)
}

/// Collects flagged types during a `syn` visit.
struct Scan {
    file: String,
    flagged: Vec<String>,
}

impl<'ast> Visit<'ast> for Scan {
    fn visit_item_struct(&mut self, i: &'ast syn::ItemStruct) {
        if derives_debug(&i.attrs) && fields_secret(&i.fields) {
            self.flagged.push(format!("{}::{}", self.file, i.ident));
        }
        syn::visit::visit_item_struct(self, i);
    }

    fn visit_item_enum(&mut self, i: &'ast syn::ItemEnum) {
        if derives_debug(&i.attrs) && i.variants.iter().any(|v| fields_secret(&v.fields)) {
            self.flagged.push(format!("{}::{}", self.file, i.ident));
        }
        syn::visit::visit_item_enum(self, i);
    }
}

/// Parses `src` and returns the list of flagged type keys.
fn flag_source(file: &str, src: &str) -> Vec<String> {
    let file_ast = syn::parse_file(src)
        .unwrap_or_else(|e| panic!("{file}: unparseable, the guard fails closed: {e}"));
    let mut scan = Scan {
        file: file.to_string(),
        flagged: Vec::new(),
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

#[test]
fn no_credential_type_derives_debug() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rs_files(&root.join("src"), &mut files);
    assert!(files.len() > 10, "src/ enumeration came back near-empty");

    let mut flagged = Vec::new();
    for path in &files {
        let rel = path
            .strip_prefix(root)
            .unwrap_or_else(|e| panic!("failed to strip prefix: {e}"))
            .to_string_lossy()
            .replace('\\', "/");
        let src = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        flagged.extend(flag_source(&rel, &src));
    }
    flagged.sort();

    let exempt_keys: Vec<&str> = EXEMPT.iter().map(|(k, _)| *k).collect();
    let unexempt: Vec<&String> = flagged
        .iter()
        .filter(|f| !exempt_keys.contains(&f.as_str()))
        .collect();
    let stale: Vec<&str> = exempt_keys
        .iter()
        .filter(|k| !flagged.iter().any(|f| f == *k))
        .copied()
        .collect();

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

#[test]
fn classifier_fixtures() {
    assert_eq!(
        flag_source(
            "f.rs",
            "#[derive(Debug, Clone)] struct A { password: String }"
        ),
        vec!["f.rs::A"]
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
        vec!["f.rs::A"]
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
        vec!["f.rs::E"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "mod m { #[derive(std::fmt::Debug)] pub struct Inner { auth_data: Vec<u8> } }"
        ),
        vec!["f.rs::Inner"]
    );

    assert_eq!(
        flag_source(
            "f.rs",
            "#[derive(Debug)] struct A { data: Option<AuthDataLike> }"
        ),
        Vec::<String>::new()
    );
}

#[test]
#[should_panic(expected = "unparseable")]
fn unparseable_source_fails_closed() {
    flag_source("f.rs", "struct {");
}
