// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use std::fmt;

use anyhow::Context;
use jiff::Timestamp;
use nautilus_core::string::secret::SecretString;
use nautilus_network::http::{
    HttpClient, HttpRedirectPolicy, HttpResponse, Method, create_standard_nautilus_headers,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    common::{credential::AlpacaCredentials, with_broker_code},
    config::AlpacaDataClientConfig,
    option_provider::{OptionCatalogEntry, load_option_catalog},
};

const RATE_LIMIT_RESET: &str = "x-ratelimit-reset";

/// Discovers active option contracts for an exact underlying, including unrecognized series.
///
/// Pass explicit expiry bounds to cover dates beyond Alpaca's default near-term window.
/// This read-only operation needs trading API credentials and no market-data connection.
///
/// # Errors
///
/// Returns an error for invalid filters, credentials, HTTP responses or catalog data.
pub async fn request_option_catalog(
    config: &AlpacaDataClientConfig,
    underlying: &str,
    root: Option<&str>,
    expiration_date_gte: Option<&str>,
    expiration_date_lte: Option<&str>,
) -> anyhow::Result<Vec<OptionCatalogEntry>> {
    let http = AlpacaHttpClient::for_trading_data(config)?;
    load_option_catalog(
        &http,
        underlying,
        root,
        expiration_date_gte,
        expiration_date_lte,
    )
    .await
}

#[derive(Debug, thiserror::Error)]
#[error("Alpaca HTTP {status}: {message}")]
pub(crate) struct AlpacaApiError {
    pub(crate) status: u16,
    pub(crate) code: Option<u64>,
    pub(crate) rate_limit_reset: Option<Timestamp>,
    pub(crate) message: String,
}

#[derive(Deserialize)]
struct ErrorResponse {
    code: Option<u64>,
    message: Option<String>,
}

#[derive(Clone)]
pub(crate) struct AlpacaHttpClient {
    client: HttpClient,
    base_url: String,
}

impl fmt::Debug for AlpacaHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(AlpacaHttpClient))
            .finish_non_exhaustive()
    }
}

impl AlpacaHttpClient {
    pub(crate) fn for_trading_data(config: &AlpacaDataClientConfig) -> anyhow::Result<Self> {
        Self::new(
            config.trading_http_url(),
            &config.credentials()?,
            config.http_timeout_secs,
            config.proxy_url.as_ref().map(SecretString::expose_secret),
        )
    }

    pub(crate) fn new(
        base_url: &str,
        credentials: &AlpacaCredentials,
        timeout_secs: u64,
        proxy_url: Option<&str>,
    ) -> anyhow::Result<Self> {
        let mut headers: std::collections::HashMap<_, _> =
            create_standard_nautilus_headers().into_iter().collect();
        headers.extend([
            ("APCA-API-KEY-ID".to_string(), credentials.key().to_owned()),
            (
                "APCA-API-SECRET-KEY".to_string(),
                credentials.secret().to_owned(),
            ),
            ("Content-Type".to_string(), "application/json".to_string()),
        ]);
        let client = HttpClient::builder()
            .redact_url(true)
            .headers(headers)
            .header_keys(vec![RATE_LIMIT_RESET.to_owned()])
            .timeout_secs(timeout_secs)
            .maybe_proxy_url(proxy_url.map(str::to_owned))
            .redirect_policy(HttpRedirectPolicy::Reject)
            .build()?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_owned(),
        })
    }

    pub(crate) async fn get<T: DeserializeOwned, Q: Serialize>(
        &self,
        path: &str,
        query: &Q,
    ) -> anyhow::Result<T> {
        let response = self
            .client
            .request_with_params(
                Method::GET,
                self.url(path),
                Some(query),
                None,
                None,
                None,
                None,
            )
            .await?;
        Self::decode(response, &Method::GET)
    }

    pub(crate) async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<T> {
        self.send(&Method::POST, path, body).await
    }

    pub(crate) async fn patch<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<T> {
        self.send(&Method::PATCH, path, body).await
    }

    pub(crate) async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let response = self
            .client
            .delete(self.url(path), None, None, None, None)
            .await?;
        Self::check_status(response, &Method::DELETE)?;
        Ok(())
    }

    async fn send<T: DeserializeOwned, B: Serialize>(
        &self,
        method: &Method,
        path: &str,
        body: &B,
    ) -> anyhow::Result<T> {
        let body = serde_json::to_vec(body)?;
        let response = self
            .client
            .request(
                method.clone(),
                self.url(path),
                None,
                None,
                Some(body),
                None,
                None,
            )
            .await?;
        Self::decode(response, method)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    fn check_status(response: HttpResponse, method: &Method) -> anyhow::Result<HttpResponse> {
        if response.status.is_success() {
            return Ok(response);
        }
        let details = serde_json::from_slice::<ErrorResponse>(&response.body).ok();
        let status = response.status.as_u16();
        let rate_limit_reset = if status == 429 {
            response
                .headers
                .get(RATE_LIMIT_RESET)
                .and_then(|value| value.parse().ok())
                .and_then(|seconds| Timestamp::from_second(seconds).ok())
        } else {
            None
        };
        let code = details.as_ref().and_then(|error| error.code);
        // Alpaca also uses 403 for order risk checks (e.g. buying power and available quantity).
        let order_refusal = method != Method::GET && code == Some(40_310_000);
        let message = match status {
            401 | 407 => with_broker_code("authentication rejected", code),
            403 if !order_refusal => with_broker_code("access forbidden", code),
            _ => details
                .and_then(|error| error.message)
                .unwrap_or_else(|| with_broker_code(&format!("HTTP {status}"), code)),
        };
        Err(AlpacaApiError {
            status,
            code,
            rate_limit_reset,
            message,
        }
        .into())
    }

    fn decode<T: DeserializeOwned>(response: HttpResponse, method: &Method) -> anyhow::Result<T> {
        let response = Self::check_status(response, method)?;
        serde_json::from_slice(&response.body).context("Invalid Alpaca response")
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(429, Some("1795450391"), Some(1_795_450_391))]
    #[case(429, None, None)]
    #[case(429, Some("soon"), None)]
    #[case(429, Some("9223372036854775807"), None)]
    #[case(503, Some("1795450391"), None)]
    fn preserves_rate_limit_reset_on_throttled_responses(
        #[case] status: u16,
        #[case] header: Option<&str>,
        #[case] expected: Option<i64>,
    ) {
        let response = HttpResponse {
            status: status.try_into().unwrap(),
            headers: header
                .map(|value| (RATE_LIMIT_RESET.to_owned(), value.to_owned()))
                .into_iter()
                .collect(),
            body: r#"{"code":42910000,"message":"broker message"}"#.into(),
        };
        let error = AlpacaHttpClient::check_status(response, &Method::GET)
            .unwrap_err()
            .downcast::<AlpacaApiError>()
            .unwrap();
        assert_eq!(error.status, status);
        assert_eq!(error.code, Some(42_910_000));
        assert_eq!(error.message, "broker message");
        assert_eq!(error.rate_limit_reset.map(|t| t.as_second()), expected);
    }

    #[rstest]
    #[case(
        401,
        Method::POST,
        Some(40_110_000),
        "credential-canary",
        "authentication rejected (code 40110000)"
    )]
    #[case(
        403,
        Method::GET,
        Some(40_310_000),
        "credential-canary",
        "access forbidden (code 40310000)"
    )]
    #[case(403, Method::POST, None, "credential-canary", "access forbidden")]
    #[case(407, Method::GET, None, "credential-canary", "authentication rejected")]
    #[case(
        403,
        Method::POST,
        Some(40_310_000),
        "insufficient buying power",
        "insufficient buying power"
    )]
    #[case(
        422,
        Method::PATCH,
        Some(42_210_000),
        "invalid limit price",
        "invalid limit price"
    )]
    fn distinguishes_authentication_from_order_errors(
        #[case] status: u16,
        #[case] method: Method,
        #[case] code: Option<u64>,
        #[case] message: &str,
        #[case] expected: &str,
    ) {
        let response = HttpResponse {
            status: status.try_into().unwrap(),
            headers: Default::default(),
            body: serde_json::json!({"code":code, "message":message})
                .to_string()
                .into(),
        };
        let err = AlpacaHttpClient::check_status(response, &method).unwrap_err();
        assert_eq!(err.to_string(), format!("Alpaca HTTP {status}: {expected}"));
        assert!(!format!("{err:?}").contains("credential-canary"));
    }

    #[tokio::test]
    async fn http_transport_and_debug_omit_endpoints_and_headers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let config = AlpacaDataClientConfig::builder()
            .api_key("credential-canary-key".into())
            .api_secret("credential-canary-secret".into())
            .base_url_http_trading(format!("http://{address}/endpoint-canary"))
            .build();
        let http = AlpacaHttpClient::for_trading_data(&config).unwrap();
        let result = http
            .get::<serde_json::Value, _>("/probe", &[("token", "query-canary")])
            .await
            .unwrap_err();
        let diagnostic = format!("{http:#?} {result:#} {result:?}");
        for forbidden in [
            "credential-canary",
            "endpoint-canary",
            "query-canary",
            &address.to_string(),
        ] {
            assert!(!diagnostic.contains(forbidden), "{diagnostic}");
        }
    }
}
