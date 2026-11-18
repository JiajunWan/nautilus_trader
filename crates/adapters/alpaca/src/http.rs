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

use anyhow::Context;
use nautilus_network::http::{
    HttpClient, HttpRedirectPolicy, HttpResponse, Method, create_standard_nautilus_headers,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::common::credential::AlpacaCredentials;

#[derive(Debug, thiserror::Error)]
#[error("Alpaca HTTP {status}: {message}")]
pub(crate) struct AlpacaApiError {
    pub(crate) status: u16,
    pub(crate) code: Option<u64>,
    pub(crate) message: String,
}

#[derive(Deserialize)]
struct ErrorResponse {
    code: Option<u64>,
    message: String,
}

#[derive(Clone, Debug)]
pub(crate) struct AlpacaHttpClient {
    client: HttpClient,
    base_url: String,
}

impl AlpacaHttpClient {
    pub(crate) fn new(
        base_url: &str,
        credentials: &AlpacaCredentials,
        timeout_secs: u64,
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
            .headers(headers)
            .timeout_secs(timeout_secs)
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
        Self::decode(response)
    }

    pub(crate) async fn post<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<T> {
        self.send(Method::POST, path, body).await
    }

    pub(crate) async fn patch<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<T> {
        self.send(Method::PATCH, path, body).await
    }

    pub(crate) async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let response = self
            .client
            .delete(self.url(path), None, None, None, None)
            .await?;
        Self::check_status(response)?;
        Ok(())
    }

    async fn send<T: DeserializeOwned, B: Serialize>(
        &self,
        method: Method,
        path: &str,
        body: &B,
    ) -> anyhow::Result<T> {
        let body = serde_json::to_vec(body)?;
        let response = self
            .client
            .request(method, self.url(path), None, None, Some(body), None, None)
            .await?;
        Self::decode(response)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    fn check_status(response: HttpResponse) -> anyhow::Result<HttpResponse> {
        if response.status.is_success() {
            return Ok(response);
        }
        let details = serde_json::from_slice::<ErrorResponse>(&response.body).ok();
        Err(AlpacaApiError {
            status: response.status.as_u16(),
            code: details.as_ref().and_then(|error| error.code),
            message: details.map_or_else(|| response.status.to_string(), |error| error.message),
        }
        .into())
    }

    fn decode<T: DeserializeOwned>(response: HttpResponse) -> anyhow::Result<T> {
        let response = Self::check_status(response)?;
        serde_json::from_slice(&response.body).context("Invalid Alpaca response")
    }
}
