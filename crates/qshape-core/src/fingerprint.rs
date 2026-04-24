use crate::error::Result;
use crate::normalize::normalize;

pub fn fingerprint(sql: &str) -> Result<String> {
    let target = normalize(sql).unwrap_or_else(|_| sql.to_string());
    let fp = pg_query::fingerprint(&target)?;

    Ok(format!("sha1:{}", fp.hex))
}

#[cfg(test)]
mod tests {
    use super::fingerprint;

    #[test]
    fn stable() {
        let a = fingerprint("SELECT id FROM users WHERE id = 1").unwrap();
        let b = fingerprint("SELECT id FROM users WHERE id = 1").unwrap();
        assert_eq!(a, b);
        assert!(a.starts_with("sha1:"), "expected sha1: prefix, got {a:?}");
    }

    #[test]
    fn ignores_whitespace() {
        let a = fingerprint("SELECT id FROM users WHERE id = 1").unwrap();
        let b = fingerprint("SELECT    id  FROM   users   WHERE  id=1").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn ignores_literals() {
        let a = fingerprint("SELECT id FROM users WHERE id = 1").unwrap();
        let b = fingerprint("SELECT id FROM users WHERE id = 42").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn distinguishes_columns() {
        let a = fingerprint("SELECT id FROM users WHERE id = 1").unwrap();
        let b = fingerprint("SELECT id FROM users WHERE name = 'x'").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn invalid_sql_errors() {
        assert!(fingerprint("SELECT FROM WHERE").is_err());
    }
}
