use axum::response::sse::Event;
use std::convert::Infallible;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;

use crate::models::{self, ChatResponseChunk, ChoiceDelta, ChunkChoice};

// Keep structural compatibility with main.rs state definitions
pub struct GeminiEngine;

pub fn combine_messages(messages: &[models::ChatMessage]) -> String {
    let mut prompt = String::new();
    for msg in messages {
        let role_label = match msg.role.as_str() {
            "system" => "System Instruction",
            "user" => "User",
            "assistant" => "Assistant",
            _ => &msg.role,
        };
        prompt.push_str(&format!("{}: {}\n\n", role_label, msg.content));
    }
    prompt
}

pub fn parse_stream_json_line(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
        if let Some(msg_type) = value.get("type") {
            if msg_type.as_str() == Some("message") {
                if let Some(role) = value.get("role") {
                    if role.as_str() == Some("assistant") {
                        if let Some(content) = value.get("content") {
                            if let Some(s) = content.as_str() {
                                return Some(s.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

pub async fn stream_prompt(
    _state: &Arc<Mutex<Option<GeminiEngine>>>,
    model: &str,
    messages: &[models::ChatMessage],
    tx: Sender<Result<Event, Infallible>>,
    created_time: u64,
) -> Result<(), String> {
    let combined_prompt = combine_messages(messages);
    let project_id =
        std::env::var("GOOGLE_CLOUD_PROJECT").unwrap_or_else(|_| "default".to_string());

    let target_model = match model {
        "gemini-cli" | "default" | "" => "gemini-3-flash",
        other => other,
    };

    let temp_dir = std::env::temp_dir();
    tracing::info!(
        "Spawning stateless isolated gemini-cli for model {} inside {:?}",
        target_model,
        temp_dir
    );

    let start_time = Instant::now();

    let temp_dir_str = temp_dir.to_string_lossy().to_string();

    let mut child = Command::new("gemini")
        .arg("--skip-trust")
        .arg("-m")
        .arg(target_model)
        .arg("-p")
        .arg(combined_prompt)
        .arg("--output-format")
        .arg("stream-json")
        .env("GOOGLE_CLOUD_PROJECT", project_id)
        .env("GEMINI_CLI_TRUST_WORKSPACE", "true")
        .env("PWD", &temp_dir_str)
        .env("INIT_CWD", &temp_dir_str)
        .current_dir(&temp_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| format!("Failed to spawn gemini subprocess: {}", e))?;

    let spawn_duration = start_time.elapsed();
    tracing::info!(
        "[Profile] gemini-cli subprocess spawned in {}ms",
        spawn_duration.as_millis()
    );

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Failed to capture child stdout".to_string())?;
    let mut reader = BufReader::new(stdout);
    let mut line_buf = String::new();

    let mut first_token_time = None;
    let mut token_count = 0;

    loop {
        line_buf.clear();
        match reader.read_line(&mut line_buf).await {
            Ok(0) => break, // EOF
            Ok(_) => {
                if let Some(text) = parse_stream_json_line(&line_buf) {
                    if first_token_time.is_none() {
                        first_token_time = Some(start_time.elapsed());
                        tracing::info!(
                            "[Profile] Time-to-First-Token (TTFT) for {}: {}ms",
                            target_model,
                            first_token_time.unwrap().as_millis()
                        );
                    }
                    token_count += 1;

                    let chunk = ChatResponseChunk {
                        id: "chatcmpl-gemini".to_string(),
                        object: "chat.completion.chunk".to_string(),
                        created: created_time,
                        model: target_model.to_string(),
                        choices: vec![ChunkChoice {
                            index: 0,
                            delta: ChoiceDelta {
                                role: None,
                                content: Some(text),
                            },
                            finish_reason: None,
                        }],
                    };

                    if let Ok(chunk_str) = serde_json::to_string(&chunk) {
                        if tx.send(Ok(Event::default().data(chunk_str))).await.is_err() {
                            break; // Client disconnected
                        }
                    }
                }
            }
            Err(e) => {
                return Err(format!("Error reading stream-json from gemini: {}", e));
            }
        }
    }

    let _ = child.wait().await;
    let total_duration = start_time.elapsed();
    let tokens_per_sec = if total_duration.as_secs_f32() > 0.0 {
        token_count as f32 / total_duration.as_secs_f32()
    } else {
        0.0
    };

    tracing::info!(
        "[Profile] Stream completed for {}: total_duration={}ms, tokens_generated={}, generation_speed={:.2} tokens/sec",
        target_model,
        total_duration.as_millis(),
        token_count,
        tokens_per_sec
    );

    Ok(())
}

pub async fn collect_prompt(
    _state: &Arc<Mutex<Option<GeminiEngine>>>,
    model: &str,
    messages: &[models::ChatMessage],
) -> Result<String, String> {
    let combined_prompt = combine_messages(messages);
    let project_id =
        std::env::var("GOOGLE_CLOUD_PROJECT").unwrap_or_else(|_| "default".to_string());

    let target_model = match model {
        "gemini-cli" | "default" | "" => "gemini-3-flash",
        other => other,
    };

    let temp_dir = std::env::temp_dir();
    tracing::info!(
        "Spawning stateless isolated gemini-cli for model {} inside {:?}",
        target_model,
        temp_dir
    );

    let start_time = Instant::now();

    let temp_dir_str = temp_dir.to_string_lossy().to_string();

    let mut child = Command::new("gemini")
        .arg("--skip-trust")
        .arg("-m")
        .arg(target_model)
        .arg("-p")
        .arg(combined_prompt)
        .arg("--output-format")
        .arg("stream-json")
        .env("GOOGLE_CLOUD_PROJECT", project_id)
        .env("GEMINI_CLI_TRUST_WORKSPACE", "true")
        .env("PWD", &temp_dir_str)
        .env("INIT_CWD", &temp_dir_str)
        .current_dir(temp_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| format!("Failed to spawn gemini subprocess: {}", e))?;

    let spawn_duration = start_time.elapsed();
    tracing::info!(
        "[Profile] gemini-cli subprocess spawned in {}ms",
        spawn_duration.as_millis()
    );

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Failed to capture child stdout".to_string())?;
    let mut reader = BufReader::new(stdout);
    let mut line_buf = String::new();
    let mut full_text = String::new();
    let mut token_count = 0;

    loop {
        line_buf.clear();
        match reader.read_line(&mut line_buf).await {
            Ok(0) => break, // EOF
            Ok(_) => {
                if let Some(text) = parse_stream_json_line(&line_buf) {
                    full_text.push_str(&text);
                    token_count += 1;
                }
            }
            Err(e) => {
                return Err(format!("Error reading stream-json from gemini: {}", e));
            }
        }
    }

    let _ = child.wait().await;
    let total_duration = start_time.elapsed();
    let tokens_per_sec = if total_duration.as_secs_f32() > 0.0 {
        token_count as f32 / total_duration.as_secs_f32()
    } else {
        0.0
    };

    tracing::info!(
        "[Profile] Collection completed for {}: total_duration={}ms, tokens_generated={}, generation_speed={:.2} tokens/sec",
        target_model,
        total_duration.as_millis(),
        token_count,
        tokens_per_sec
    );

    Ok(full_text)
}
