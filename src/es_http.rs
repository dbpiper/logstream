use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::{Client, Method, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::es_url;

#[derive(Clone)]
pub struct EsHttp {
    client: Client,
    base_url: Arc<str>,
    user: Arc<str>,
    pass: Arc<str>,
}

impl EsHttp {
    pub fn new(
        base_url: impl Into<Arc<str>>,
        user: impl Into<Arc<str>>,
        pass: impl Into<Arc<str>>,
        timeout: Duration,
        gzip: bool,
    ) -> Result<Self> {
        let client = Client::builder().timeout(timeout).gzip(gzip).build()?;
        Ok(Self {
            client,
            base_url: es_url::normalize_base_url(base_url),
            user: user.into(),
            pass: pass.into(),
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn pass(&self) -> &str {
        &self.pass
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }

    pub fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, self.url(path))
            .basic_auth(&*self.user, Some(&*self.pass))
    }

    pub async fn send_expect(
        &self,
        req: reqwest::RequestBuilder,
        context: &'static str,
        ok: impl FnOnce(StatusCode) -> bool,
    ) -> Result<Response> {
        let resp = req.send().await.context(context)?;
        let status = resp.status();
        if ok(status) {
            return Ok(resp);
        }

        let text = resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "{} status={} body_sample={}",
            context,
            status,
            truncate_body_snippet(&text, 500)
        );
    }

    pub async fn send_ok(
        &self,
        req: reqwest::RequestBuilder,
        context: &'static str,
    ) -> Result<Response> {
        self.send_expect(req, context, |s| s.is_success()).await
    }

    pub async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        context: &'static str,
    ) -> Result<T> {
        let resp = self
            .send_ok(self.request(Method::GET, path), context)
            .await?;
        resp.json().await.context(context)
    }

    pub async fn get_value(&self, path: &str, context: &'static str) -> Result<Value> {
        self.get_json(path, context).await
    }

    pub async fn post_json<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &Value,
        context: &'static str,
    ) -> Result<T> {
        let resp = self
            .send_ok(self.request(Method::POST, path).json(body), context)
            .await?;
        resp.json().await.context(context)
    }

    pub async fn post_value(
        &self,
        path: &str,
        body: &Value,
        context: &'static str,
    ) -> Result<Value> {
        self.post_json(path, body, context).await
    }

    pub async fn put_value(
        &self,
        path: &str,
        body: &Value,
        context: &'static str,
    ) -> Result<Value> {
        let resp = self
            .send_ok(self.request(Method::PUT, path).json(body), context)
            .await?;
        resp.json().await.context(context)
    }

    pub async fn delete_allow_404(&self, path: &str, context: &'static str) -> Result<()> {
        let _ = self
            .send_expect(self.request(Method::DELETE, path), context, |s| {
                s.is_success() || s.as_u16() == 404
            })
            .await?;
        Ok(())
    }
}

fn truncate_body_snippet(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        return s.to_string();
    }
    format!("{}…", &s[..max_len])
}
