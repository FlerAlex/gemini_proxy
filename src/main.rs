mod engine;
mod models;

use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures_util::stream::Stream;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

use engine::{collect_prompt, stream_prompt, GeminiEngine};
use models::{ChatMessage, ChatResponse, OpenAIRequest, ResponseChoice, Usage};

// A custom stream wrapper to convert an mpsc::Receiver into a Stream without extra dependencies.
struct ChannelStream {
    rx: tokio::sync::mpsc::Receiver<Result<Event, Infallible>>,
}

impl Stream for ChannelStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.contains(&"-v".to_string()) || args.contains(&"--version".to_string()) {
        println!("gemini-cli-openai-proxy v{}", env!("CARGO_PKG_VERSION"));
        return;
    }

    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let state = Arc::new(Mutex::new(None));

    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8765".to_string())
        .parse::<u16>()
        .unwrap_or(8765);

    let bind_ip_str = std::env::var("BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1".to_string());
    let bind_ip = bind_ip_str
        .parse::<std::net::IpAddr>()
        .unwrap_or_else(|_| std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)));

    let addr = std::net::SocketAddr::new(bind_ip, port);
    tracing::info!(
        "Starting Rust OpenAI-to-Gemini-CLI MCP Proxy on http://{}",
        addr
    );

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/v1/models", get(handle_models))
        .route("/v1/chat/completions", post(handle_chat_completions))
        .layer(cors)
        .with_state(state.clone());

    let shutdown_signal = async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl_c signal");

        tracing::info!("Shutdown signal received. Exiting...");
    };

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal)
        .await
        .unwrap();
}

async fn health_check() -> &'static str {
    "OK"
}

async fn handle_models() -> impl IntoResponse {
    let models = serde_json::json!({
        "object": "list",
        "data": [
            {
                "id": "gemini-cli",
                "object": "model",
                "created": 1717113600,
                "owned_by": "google"
            },
            {
                "id": "gemini-3-flash",
                "object": "model",
                "created": 1717113600,
                "owned_by": "google"
            },
            {
                "id": "gemini-3.1-flash-lite",
                "object": "model",
                "created": 1717113600,
                "owned_by": "google"
            },
            {
                "id": "gemini-1.5-pro",
                "object": "model",
                "created": 1717113600,
                "owned_by": "google"
            },
            {
                "id": "gemini-2.5-pro",
                "object": "model",
                "created": 1717113600,
                "owned_by": "google"
            },
            {
                "id": "gemini-3.1-pro-preview",
                "object": "model",
                "created": 1717113600,
                "owned_by": "google"
            }
        ]
    });
    Json(models)
}

async fn handle_chat_completions(
    State(state): State<Arc<Mutex<Option<GeminiEngine>>>>,
    Json(req): Json<OpenAIRequest>,
) -> Response {
    if req.stream {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(100);
        let state_clone = state.clone();

        tokio::spawn(async move {
            let created_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            if let Err(e) = stream_prompt(
                &state_clone,
                &req.model,
                &req.messages,
                tx.clone(),
                created_time,
            )
            .await
            {
                let _ = tx
                    .send(Ok(Event::default().data(format!("Error: {}", e))))
                    .await;
            }

            let _ = tx.send(Ok(Event::default().data("[DONE]"))).await;
        });

        let stream = ChannelStream { rx };
        Sse::new(stream)
            .keep_alive(KeepAlive::default())
            .into_response()
    } else {
        let state_clone = state.clone();
        match collect_prompt(&state_clone, &req.model, &req.messages).await {
            Ok(full_text) => {
                let created_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                let response = ChatResponse {
                    id: "chatcmpl-gemini".to_string(),
                    object: "chat.completion".to_string(),
                    created: created_time,
                    model: req.model.clone(),
                    choices: vec![ResponseChoice {
                        index: 0,
                        message: ChatMessage {
                            role: "assistant".to_string(),
                            content: full_text,
                        },
                        finish_reason: Some("stop".to_string()),
                    }],
                    usage: Some(Usage {
                        prompt_tokens: 0,
                        completion_tokens: 0,
                        total_tokens: 0,
                    }),
                };
                Json(response).into_response()
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Engine execution error: {}", e),
            )
                .into_response(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use models::ChatMessage;

    #[test]
    fn test_combine_messages() {
        let messages = vec![
            ChatMessage {
                role: "system".to_string(),
                content: "System instructions here".to_string(),
            },
            ChatMessage {
                role: "user".to_string(),
                content: "Hello".to_string(),
            },
        ];
        let combined = engine::combine_messages(&messages);
        assert!(combined.contains("System Instruction: System instructions here"));
        assert!(combined.contains("User: Hello"));
    }

    #[test]
    fn test_parse_stream_json_line_valid() {
        let line = r#"{"type":"message","timestamp":"2026-06-01T01:34:57.089Z","role":"assistant","content":"This is a token chunk.","delta":true}"#;
        let parsed = engine::parse_stream_json_line(line).unwrap();
        assert_eq!(parsed, "This is a token chunk.");
    }

    #[test]
    fn test_parse_stream_json_line_other_type() {
        let line = r#"{"type":"tool_use","tool_name":"some_tool"}"#;
        let parsed = engine::parse_stream_json_line(line);
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_stream_json_line_invalid() {
        let line = "not a json line";
        let parsed = engine::parse_stream_json_line(line);
        assert!(parsed.is_none());
    }

    #[tokio::test]
    async fn test_handle_models() {
        let response = handle_models().await.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }
}
