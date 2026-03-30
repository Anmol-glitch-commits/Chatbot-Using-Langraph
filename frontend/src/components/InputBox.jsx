import { useRef } from "react";
import { createThread, sendMessage, uploadPdf } from "../api/api";

export default function InputBox({
	message,
	setMessage,
	activeId,
	setThreadId,
	setMessages,
	setThreadList,
	isLoading,
	setIsLoading,
}) {
	const fileInputRef = useRef(null);

	async function handleSubmit(e) {
		e.preventDefault();

		if (!message.trim() || isLoading) {
			return;
		}

		const userMessage = message.trim();

		// Optimistically add the user message to the chat
		setMessages((prev) => [
			...prev,
			{
				role: "user",
				content: userMessage,
				timestamp: new Date().toISOString(),
			},
		]);
		setMessage("");
		setIsLoading(true);

		try {
			const res = await sendMessage(userMessage, activeId);

			// If this was a new chat (no activeId), set the thread ID
			if (!activeId && res.thread_id) {
				setThreadId(res.thread_id);

				// Add the new thread to the sidebar list
				setThreadList((prev) => [
					{
						id: res.thread_id,
						message: userMessage,
						aiMessage: res.response,
					},
					...prev,
				]);
			}

			// Add the AI response to messages
			setMessages((prev) => [
				...prev,
				{
					role: "assistant",
					content: res.response,
					timestamp: res.ai_timestamp || new Date().toISOString(),
				},
			]);

			// Move the active thread to the top of the sidebar
			if (activeId) {
				setThreadList((prev) => {
					const thread = prev.find((t) => t.id === activeId);
					if (thread) {
						return [thread, ...prev.filter((t) => t.id !== activeId)];
					}
					return prev;
				});
			}
		} catch (error) {
			console.error(error);
			// Remove the optimistic user message on error
			setMessages((prev) => prev.slice(0, -1));
		} finally {
			setIsLoading(false);
		}
	}

	async function handleFileUpload(e) {
		const file = e.target.files?.[0];
		if (!file) return;

		let currentThreadId = activeId;
		if (!currentThreadId) {
			const { thread_id } = await createThread();
			currentThreadId = thread_id;
			setThreadId(currentThreadId);
		}
		setIsLoading(true);
		try {
			const result = await uploadPdf(file, currentThreadId);

			setMessages((prev) => [
				...prev,
				{
					role: "assistant",
					content: `📄 PDF "${result.filename}" uploaded — ${result.documents} pages, ${result.chunks} chunks indexed.`,
					timestamp: new Date().toISOString(),
				},
			]);
			setThreadList((prev) => [
				...prev,
				{
					id: currentThreadId,
					message: result.filename,
					aiMessage: "Ask me questions related to pdf",
				},
			]);
		} catch (err) {
			console.error(err);
		} finally {
			setIsLoading(false);
			e.target.value = "";
		}
	}

	return (
		<div className="input-area">
			<div className="input-container">
				<button
					className="input-attach-btn"
					title="Attach file"
					type="button"
					onClick={() => fileInputRef.current?.click()}
				>
					<svg
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						strokeWidth="2"
						strokeLinecap="round"
						strokeLinejoin="round"
					>
						<path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48" />
					</svg>
				</button>
				<input
					ref={fileInputRef}
					type="file"
					accept=".pdf,application/pdf"
					style={{ display: "none" }}
					onChange={handleFileUpload}
				/>
				<input
					type="text"
					className="input-field"
					placeholder="Type your message..."
					id="chat-input"
					value={message}
					onChange={(e) => setMessage(e.target.value)}
					onKeyDown={(e) => {
						if (e.key === "Enter" && !e.shiftKey) {
							handleSubmit(e);
						}
					}}
					disabled={isLoading}
				/>
				<button
					className="input-send-btn"
					title="Send message"
					id="send-button"
					type="submit"
					onClick={handleSubmit}
					disabled={isLoading}
				>
					{isLoading ? (
						<svg
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							strokeWidth="2"
							className="loading-spinner"
						>
							<circle cx="12" cy="12" r="10" opacity="0.25" />
							<path d="M12 2a10 10 0 0 1 10 10" />
						</svg>
					) : (
						<svg
							viewBox="0 0 24 24"
							fill="none"
							stroke="currentColor"
							strokeWidth="2"
							strokeLinecap="round"
							strokeLinejoin="round"
						>
							<line x1="22" y1="2" x2="11" y2="13" />
							<polygon points="22 2 15 22 11 13 2 9 22 2" />
						</svg>
					)}
				</button>
			</div>
			<p className="input-disclaimer">
				AI may produce inaccurate information. Verify important details.
			</p>
		</div>
	);
}
