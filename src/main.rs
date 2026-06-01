mod engine;
mod models;

use axum::{
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
use std::task::{Context, Poll};
use tower_http::cors::{Any, CorsLayer};

use engine::{collect_prompt, stream_prompt};
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
        .layer(cors);

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
    Json(req): Json<OpenAIRequest>,
) -> Response {
    if let Some(tools) = &req.tools {
        let is_valid_empty = tools.is_null() || tools.as_array().is_some_and(|arr| arr.is_empty());
        if !is_valid_empty {
            return (
                StatusCode::BAD_REQUEST,
                "Tool calls are not supported by the stateless gemini-cli proxy execution model.",
            )
                .into_response();
        }
    }

    if req.temperature.is_some() || req.max_tokens.is_some() {
        tracing::warn!(
            "Request contains ignored OpenAI parameters: temperature={:?}, max_tokens={:?}",
            req.temperature,
            req.max_tokens
        );
    }

    let binary = std::env::var("GEMINI_BINARY").unwrap_or_else(|_| "gemini".to_string());

    if req.stream {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(100);
        let binary_clone = binary.clone();

        tokio::spawn(async move {
            let created_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            if let Err(e) = stream_prompt(
                &binary_clone,
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
        match collect_prompt(&binary, &req.model, &req.messages).await {
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

    #[tokio::test]
    async fn test_handle_chat_completions_tools_rejection() {
        let req = OpenAIRequest {
            model: "gemini-3-flash".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: "Hello".to_string(),
            }],
            stream: false,
            temperature: None,
            max_tokens: None,
            tools: Some(serde_json::json!([
                {
                    "type": "function",
                    "function": {
                        "name": "get_current_weather",
                        "description": "Get the current weather",
                    }
                }
            ])),
        };

        let response = handle_chat_completions(Json(req)).await;
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }



    #[tokio::test]
    async fn test_collect_prompt_with_mock_binary() {
        let messages = vec![ChatMessage {
            role: "user".to_string(),
            content: "Hello".to_string(),
        }];
        let result = collect_prompt("echo", "gemini-3-flash", &messages).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[tokio::test]
    async fn test_handle_chat_completions_tools_non_array_rejection() {
        let req = OpenAIRequest {
            model: "gemini-3-flash".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: "Hello".to_string(),
            }],
            stream: false,
            temperature: None,
            max_tokens: None,
            tools: Some(serde_json::json!({
                "type": "code_interpreter"
            })),
        };

        let response = handle_chat_completions(Json(req)).await;
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    }
}
