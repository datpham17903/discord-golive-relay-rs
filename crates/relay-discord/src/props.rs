//! Client properties and header context, port of `discord/tracking.py::HeadersContext` (Chrome on
//! Windows only, the library default).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use wreq_util::{Emulation, Platform, Profile};

use crate::error::{Error, Result};

/// `INTERNAL_API_VERSION` in `discord/http.py`.
pub const API_VERSION: u32 = 9;
pub const FALLBACK_BUILD_NUMBER: u64 = 9999;
pub const FALLBACK_BROWSER_VERSION: u32 = 136;
/// `asyncio.wait_for(fetch_api_properties, timeout=3)` in `HeadersContext.default`.
pub const API_PROPERTIES_TIMEOUT: Duration = Duration::from_secs(3);

const USER_AGENT: &str = concat!("golive-relay/", env!("CARGO_PKG_VERSION"));
const LAUNCH_SIGNATURE_BITS: &str = "00000000100000000001000000010000000010000001000000001000000000000010000010000001000000000100000000000001000000000000100000000000";

/// Every endpoint the client talks to; injectable so tests can point it at local fakes.
#[derive(Debug, Clone)]
pub struct Urls {
    /// `https://discord.com`; also the `Origin` header (`BASE_DOMAIN` in the Python client).
    pub origin: String,
    /// REST base including the API version.
    pub api_base: String,
    pub gateway: String,
    /// Info API; `/api/v2/properties/web` is appended.
    pub cordapi: String,
    /// Page scraped for `BUILD_NUMBER` when the info API is down.
    pub login_page: String,
    pub chrome_versions: String,
}

impl Default for Urls {
    fn default() -> Self {
        Self {
            origin: "https://discord.com".into(),
            api_base: format!("https://discord.com/api/v{API_VERSION}"),
            gateway: "wss://gateway.discord.gg/".into(),
            cordapi: "https://cordapi.dolfi.es".into(),
            login_page: "https://discord.com/login".into(),
            chrome_versions: "https://versionhistory.googleapis.com/v1/chrome/platforms/win/channels/stable/versions"
                .into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HeadersContext {
    pub super_properties: Map<String, Value>,
    pub encoded_super_properties: String,
    pub extra_gateway_properties: Map<String, Value>,
    pub browser_major_version: u32,
    pub initialization_timestamp_ms: u64,
}

#[derive(Deserialize)]
struct ApiProperties {
    properties: Map<String, Value>,
    extra_gateway_properties: Option<Map<String, Value>>,
    encoded: Option<String>,
}

impl HeadersContext {
    pub fn new(
        super_properties: Map<String, Value>,
        encoded: Option<String>,
        extra_gateway_properties: Option<Map<String, Value>>,
        browser_major_version: u32,
    ) -> Self {
        let encoded_super_properties = encoded.filter(|e| !e.is_empty()).unwrap_or_else(|| {
            STANDARD.encode(Value::Object(super_properties.clone()).to_string())
        });
        Self {
            super_properties,
            encoded_super_properties,
            extra_gateway_properties: extra_gateway_properties.unwrap_or_default(),
            browser_major_version,
            initialization_timestamp_ms: now_ms(),
        }
    }

    /// `HeadersContext.default`: info API (3 s), else scrape the build number and Chrome version,
    /// else hardcoded constants. Never fails.
    pub async fn fetch_default(http: &wreq::Client, urls: &Urls) -> Self {
        Self::fetch_default_with_timeout(http, urls, API_PROPERTIES_TIMEOUT).await
    }

    pub async fn fetch_default_with_timeout(
        http: &wreq::Client,
        urls: &Urls,
        timeout: Duration,
    ) -> Self {
        if let Ok(Ok(data)) =
            tokio::time::timeout(timeout, Self::fetch_api_properties(http, urls)).await
            && let Some(major) = major_version(data.properties.get("browser_version"))
        {
            return Self::new(
                data.properties,
                data.encoded,
                data.extra_gateway_properties,
                major,
            );
        }

        let build_number = Self::scrape_client_build_number(http, urls)
            .await
            .unwrap_or(FALLBACK_BUILD_NUMBER);
        let major = match Self::fetch_chrome_version(http, urls).await {
            Ok(v) => major_version(Some(&Value::String(v))).unwrap_or_else(latest_emulated_major),
            Err(_) => latest_emulated_major(),
        };
        let mut props = Map::new();
        for (k, v) in [
            ("os", json!("Windows")),
            ("browser", json!("Chrome")),
            ("device", json!("")),
            ("system_locale", json!("en-US")),
            (
                "browser_user_agent",
                json!(format_chromium_user_agent(major)),
            ),
            ("browser_version", json!(format!("{major}.0.0.0"))),
            ("os_version", json!("10")),
            ("referrer", json!("")),
            ("referring_domain", json!("")),
            ("referrer_current", json!("")),
            ("referring_domain_current", json!("")),
            ("release_channel", json!("stable")),
            ("client_build_number", json!(build_number)),
            ("client_event_source", Value::Null),
            ("has_client_mods", json!(false)),
            ("client_launch_id", json!(uuid::Uuid::new_v4().to_string())),
            ("client_app_state", json!("unfocused")),
            (
                "client_heartbeat_session_id",
                json!(uuid::Uuid::new_v4().to_string()),
            ),
            ("launch_signature", json!(generate_launch_signature())),
        ] {
            props.insert(k.into(), v);
        }
        let mut extra = Map::new();
        extra.insert("is_fast_connect".into(), json!(false));
        extra.insert("gateway_connect_reasons".into(), json!("AppSkeleton"));
        Self::new(props, None, Some(extra), major)
    }

    /// `POST {cordapi}/api/v2/properties/web`; 202 means "temporarily unavailable".
    async fn fetch_api_properties(http: &wreq::Client, urls: &Urls) -> Result<ApiProperties> {
        let resp = http
            .post(format!(
                "{}/api/v2/properties/web",
                urls.cordapi.trim_end_matches('/')
            ))
            .header("User-Agent", USER_AGENT)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() || status.as_u16() == 202 {
            return Err(Error::Status {
                status: status.as_u16(),
                message: "info API unavailable".into(),
            });
        }
        Ok(resp.json().await?)
    }

    pub async fn scrape_client_build_number(http: &wreq::Client, urls: &Urls) -> Result<u64> {
        let page = http.get(&urls.login_page).send().await?.text().await?;
        find_build_number(&page)
            .ok_or_else(|| Error::InvalidData("could not find client global env".into()))
    }

    pub async fn fetch_chrome_version(http: &wreq::Client, urls: &Urls) -> Result<String> {
        let resp = http.get(&urls.chrome_versions).send().await?;
        if !resp.status().is_success() {
            return Err(Error::Status {
                status: resp.status().as_u16(),
                message: "chrome version API".into(),
            });
        }
        let data: Value = resp.json().await?;
        data["versions"][0]["version"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| Error::InvalidData("no chrome version in response".into()))
    }

    pub fn user_agent(&self) -> &str {
        self.super_properties
            .get("browser_user_agent")
            .and_then(Value::as_str)
            .unwrap_or_default()
    }

    pub fn build_number(&self) -> Option<u64> {
        self.super_properties
            .get("client_build_number")
            .and_then(Value::as_u64)
    }

    /// `{**super_properties, **extra_gateway_properties}`.
    pub fn gateway_properties(&self) -> Map<String, Value> {
        let mut props = self.super_properties.clone();
        props.extend(self.extra_gateway_properties.clone());
        props
    }

    /// Highest wreq-util Chrome profile whose major is not above the claimed browser version.
    pub fn emulation(&self) -> Emulation {
        Emulation::builder()
            .profile(emulation_for_major(self.browser_major_version))
            .platform(Platform::Windows)
            .build()
    }

    /// `HeadersContext.client_hints` for Chrome on desktop.
    pub fn client_hints(&self) -> Vec<(&'static str, String)> {
        let version = self.browser_major_version;
        let list = [
            greased_brand_version(version),
            ("Chromium".to_owned(), version.to_string()),
            ("Google Chrome".to_owned(), version.to_string()),
        ];
        let mut shuffled: [Option<(String, String)>; 3] = [None, None, None];
        for (entry, idx) in list.into_iter().zip(random_order3(version)) {
            shuffled[idx] = Some(entry);
        }
        let sec_ch_ua = shuffled
            .into_iter()
            .flatten()
            .map(|(brand, v)| format!("\"{brand}\";v=\"{v}\""))
            .collect::<Vec<_>>()
            .join(", ");
        vec![
            ("Priority", "u=0, i".into()),
            ("Sec-CH-UA", sec_ch_ua),
            ("Sec-CH-UA-Mobile", "?0".into()),
            ("Sec-CH-UA-Platform", "\"Windows\"".into()),
        ]
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn major_version(v: Option<&Value>) -> Option<u32> {
    v?.as_str()?.split('.').next()?.parse().ok()
}

pub fn format_chromium_user_agent(version: u32) -> String {
    format!(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version}.0.0.0 Safari/537.36"
    )
}

/// UUID4 with the fixed zero bits of `generate_launch_signature`.
pub fn generate_launch_signature() -> String {
    let bits = u128::from_str_radix(LAUNCH_SIGNATURE_BITS, 2).expect("constant is binary");
    let random = uuid::Uuid::new_v4().as_u128() & !bits;
    uuid::Uuid::from_u128(random).to_string()
}

/// `"BUILD_NUMBER":\s*"(\d+)"`
fn find_build_number(page: &str) -> Option<u64> {
    const KEY: &str = "\"BUILD_NUMBER\":";
    page.match_indices(KEY).find_map(|(i, _)| {
        let rest = page[i + KEY.len()..].trim_start().strip_prefix('"')?;
        let digits = rest.split('"').next()?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok()
    })
}

fn greased_brand_version(seed: u32) -> (String, String) {
    const CHARS: [char; 11] = [' ', '(', ':', '-', '.', '/', ')', ';', '=', '?', '_'];
    const VERSIONS: [&str; 3] = ["8", "99", "24"];
    let s = seed as usize;
    (
        format!(
            "Not{}A{}Brand",
            CHARS[s % CHARS.len()],
            CHARS[(s + 1) % CHARS.len()]
        ),
        VERSIONS[s % VERSIONS.len()].to_owned(),
    )
}

fn random_order3(seed: u32) -> [usize; 3] {
    const ORDERS: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    ORDERS[seed as usize % ORDERS.len()]
}

const CHROME_PROFILES: &[(u32, Profile)] = &[
    (100, Profile::Chrome100),
    (101, Profile::Chrome101),
    (104, Profile::Chrome104),
    (105, Profile::Chrome105),
    (106, Profile::Chrome106),
    (107, Profile::Chrome107),
    (108, Profile::Chrome108),
    (109, Profile::Chrome109),
    (110, Profile::Chrome110),
    (114, Profile::Chrome114),
    (116, Profile::Chrome116),
    (117, Profile::Chrome117),
    (118, Profile::Chrome118),
    (119, Profile::Chrome119),
    (120, Profile::Chrome120),
    (123, Profile::Chrome123),
    (124, Profile::Chrome124),
    (126, Profile::Chrome126),
    (127, Profile::Chrome127),
    (128, Profile::Chrome128),
    (129, Profile::Chrome129),
    (130, Profile::Chrome130),
    (131, Profile::Chrome131),
    (132, Profile::Chrome132),
    (133, Profile::Chrome133),
    (134, Profile::Chrome134),
    (135, Profile::Chrome135),
    (136, Profile::Chrome136),
    (137, Profile::Chrome137),
    (138, Profile::Chrome138),
    (139, Profile::Chrome139),
    (140, Profile::Chrome140),
    (141, Profile::Chrome141),
    (142, Profile::Chrome142),
    (143, Profile::Chrome143),
    (144, Profile::Chrome144),
    (145, Profile::Chrome145),
    (146, Profile::Chrome146),
    (147, Profile::Chrome147),
    (148, Profile::Chrome148),
    (149, Profile::Chrome149),
];

pub fn emulation_for_major(major: u32) -> Profile {
    CHROME_PROFILES
        .iter()
        .rev()
        .find(|(m, _)| *m <= major)
        .map_or(CHROME_PROFILES[0].1, |(_, e)| *e)
}

fn latest_emulated_major() -> u32 {
    CHROME_PROFILES
        .last()
        .map_or(FALLBACK_BROWSER_VERSION, |(m, _)| *m)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emulation_picks_highest_not_above() {
        assert_eq!(emulation_for_major(158), Profile::Chrome149);
        assert_eq!(emulation_for_major(149), Profile::Chrome149);
        assert_eq!(emulation_for_major(125), Profile::Chrome124);
        assert_eq!(emulation_for_major(111), Profile::Chrome110);
        assert_eq!(emulation_for_major(50), Profile::Chrome100);
    }

    #[test]
    fn build_number_regex_semantics() {
        assert_eq!(
            find_build_number(r#"x "BUILD_NUMBER":  "12345", y"#),
            Some(12345)
        );
        assert_eq!(
            find_build_number(r#""BUILD_NUMBER":"abc" "BUILD_NUMBER": "7""#),
            Some(7)
        );
        assert_eq!(find_build_number("no number here"), None);
    }

    #[test]
    fn launch_signature_is_uuid_with_zero_bits() {
        let bits = u128::from_str_radix(LAUNCH_SIGNATURE_BITS, 2).unwrap();
        for _ in 0..50 {
            let sig = generate_launch_signature();
            let n = uuid::Uuid::parse_str(&sig).unwrap().as_u128();
            assert_eq!(n & bits, 0);
        }
    }

    #[test]
    fn client_hints_match_python() {
        // Expected strings generated by the pinned discord.py-self `HeadersContext.client_hints`.
        for (version, expected) in [
            (
                136,
                r#""Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99""#,
            ),
            (
                149,
                r#""Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24""#,
            ),
            (
                158,
                r#""Chromium";v="158", "Not.A/Brand";v="24", "Google Chrome";v="158""#,
            ),
        ] {
            let hints = HeadersContext::new(Map::new(), None, None, version).client_hints();
            let ua = &hints.iter().find(|(k, _)| *k == "Sec-CH-UA").unwrap().1;
            assert_eq!(ua, expected, "{version}");
        }
    }

    #[test]
    fn encoded_falls_back_to_base64_of_json() {
        let mut p = Map::new();
        p.insert("a".into(), json!(1));
        let ctx = HeadersContext::new(p, None, None, 136);
        assert_eq!(
            STANDARD.decode(&ctx.encoded_super_properties).unwrap(),
            br#"{"a":1}"#
        );
    }
}
