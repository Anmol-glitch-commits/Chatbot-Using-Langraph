import { useEffect } from "react";
import { getThreadMessages } from "../api/api";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
export default function MainChat({ activeId, messages, setMessages, isLoading }) {
	useEffect(() => {
		async function fetchMessages() {
			if (!activeId) {
				return;
			}

			try {
				const res = await getThreadMessages(activeId);
				setMessages(res.messages);
			} catch (error) {
				console.error(error);
			}
		}
		fetchMessages();
	}, [activeId]);

	return (
		<main className="chat-area">
			<div className="chat-messages">
				{/* Welcome message */}
				{messages.length === 0 && (
					<div className="chat-welcome">
						<div className="chat-welcome-icon">
							<svg
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								strokeWidth="1.5"
								strokeLinecap="round"
								strokeLinejoin="round"
							>
								<path d="M12 2a7 7 0 0 1 7 7c0 2.38-1.19 4.47-3 5.74V17a2 2 0 0 1-2 2H10a2 2 0 0 1-2-2v-2.26C6.19 13.47 5 11.38 5 9a7 7 0 0 1 7-7z" />
								<line x1="10" y1="22" x2="14" y2="22" />
								<line x1="12" y1="19" x2="12" y2="22" />
							</svg>
						</div>
						<h2 className="chat-welcome-title">How can I help you today?</h2>
						<p className="chat-welcome-subtitle">
							Ask me anything — I'm here to assist.
						</p>
					</div>
				)}

				{/* Example conversation messages */}
				{messages?.map((msg, index) => {
					return (
						<div
							className={`message ${msg.role === "user" ? "message-user" : "message-bot"}`}
							key={index}
						>
							{msg.role === "assistant" && (
								<div className="message-avatar">
									<svg
										viewBox="0 0 24 24"
										fill="none"
										stroke="currentColor"
										strokeWidth="1.5"
										strokeLinecap="round"
										strokeLinejoin="round"
									>
										<path d="M12 2a7 7 0 0 1 7 7c0 2.38-1.19 4.47-3 5.74V17a2 2 0 0 1-2 2H10a2 2 0 0 1-2-2v-2.26C6.19 13.47 5 11.38 5 9a7 7 0 0 1 7-7z" />
										<line x1="10" y1="22" x2="14" y2="22" />
										<line x1="12" y1="19" x2="12" y2="22" />
									</svg>
								</div>
							)}
							<div className="message-bubble">
								<Markdown remarkPlugins={[remarkGfm]}>{msg.content}</Markdown>
							</div>
							<span className="message-time">
								{msg.timestamp
									? new Date(msg.timestamp).toLocaleTimeString([], {
											hour: "numeric",
											minute: "2-digit",
											hour12: true,
										})
									: ""}
							</span>
						</div>
					);
				})}

				{/* Typing indicator */}
				{isLoading && (
					<div className="message message-bot">
						<div className="message-avatar">
							<svg
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								strokeWidth="1.5"
								strokeLinecap="round"
								strokeLinejoin="round"
							>
								<path d="M12 2a7 7 0 0 1 7 7c0 2.38-1.19 4.47-3 5.74V17a2 2 0 0 1-2 2H10a2 2 0 0 1-2-2v-2.26C6.19 13.47 5 11.38 5 9a7 7 0 0 1 7-7z" />
								<line x1="10" y1="22" x2="14" y2="22" />
								<line x1="12" y1="19" x2="12" y2="22" />
							</svg>
						</div>
						<div className="message-bubble typing-bubble">
							<div className="typing-indicator">
								<span className="typing-dot"></span>
								<span className="typing-dot"></span>
								<span className="typing-dot"></span>
							</div>
						</div>
					</div>
				)}
			</div>
		</main>
	);
}
