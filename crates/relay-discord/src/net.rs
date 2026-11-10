//! The network side shared by the ingest adapter and the output accounts: REST client, main gateway
//! handle and the pieces the voice clients need (gateway link, HTTP client, user agent).

use std::sync::Arc;

use relay_voice::link::GatewayLink;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::link::GatewayLinkImpl;
use crate::{Gateway, GatewayConfig, GatewayEvent, GatewayHandle, RestClient, SharedCache};

#[derive(Clone)]
pub(crate) struct Net {
    pub link: Arc<dyn GatewayLink>,
    pub http: wreq::Client,
    pub user_agent: String,
    pub rest: Option<RestClient>,
    pub gateway: Option<GatewayHandle>,
    /// Gateway session id when there is no gateway (tests).
    pub fixed_session: Option<String>,
}

impl Net {
    /// Starts the gateway on `rest`'s client (one browser fingerprint for REST, gateway and voice
    /// websockets), applying dispatches to `cache`.
    pub fn attach(rest: RestClient, cache: SharedCache) -> (Self, UnboundedReceiver<GatewayEvent>) {
        let (gateway, events) = Gateway::spawn_with_cache(GatewayConfig::from_rest(&rest), cache);
        let net = Self {
            link: Arc::new(GatewayLinkImpl {
                gateway: gateway.clone(),
            }),
            http: rest.http_client().clone(),
            user_agent: rest.headers_context().user_agent().to_owned(),
            rest: Some(rest),
            gateway: Some(gateway),
            fixed_session: None,
        };
        (net, events)
    }

    /// A net without a gateway or REST client, for tests of the voice-facing paths.
    #[cfg(test)]
    pub fn detached(link: Arc<dyn GatewayLink>, session: Option<&str>) -> Self {
        Self {
            link,
            http: wreq::Client::builder().build().expect("http client"),
            user_agent: "test".to_owned(),
            rest: None,
            gateway: None,
            fixed_session: session.map(str::to_owned),
        }
    }

    pub fn gateway_session(&self) -> Option<String> {
        match &self.gateway {
            Some(gateway) => gateway.session_id(),
            None => self.fixed_session.clone(),
        }
    }

    pub fn set_rtc_connected(&self, connected: bool) {
        if let Some(gateway) = &self.gateway {
            gateway.set_rtc_connected(connected);
        }
    }

    pub fn close(&self) {
        if let Some(gateway) = &self.gateway {
            gateway.set_rtc_connected(false);
            gateway.close();
        }
    }
}
