import { useEffect, useState } from "react";
import { deleteThread } from "../api/api";

export default function Sidebar({
	threadList,
	setThreadId,
	activeId,
	setMessages,
	setThreadList,
	isOpen,
	onClose,
}) {
	const [filteredList, setFilteredList] = useState(threadList);
	const [searchQuery, setSearchQuery] = useState("");

	useEffect(() => {
		setFilteredList(
			threadList.filter((thread) => {
				return (
					thread.message.toLowerCase().includes(searchQuery.toLowerCase()) ||
					thread.aiMessage.toLowerCase().includes(searchQuery.toLowerCase())
				);
			}),
		);
	}, [searchQuery, threadList]);

	function handleNewChat() {
		setThreadId("");
		setSearchQuery("");
		setMessages([]);
		onClose?.();
	}

	function handleSelectThread(id) {
		setThreadId(id);
		onClose?.();
	}

	async function handleDeleteThread(e, threadId) {
		e.stopPropagation(); // Prevent selecting the thread when clicking delete
		try {
			await deleteThread(threadId);
			setThreadList((prev) => prev.filter((t) => t.id !== threadId));
			// If we deleted the active thread, clear the chat
			if (threadId === activeId) {
				setThreadId("");
				setMessages([]);
			}
		} catch (error) {
			console.error("Failed to delete thread:", error);
		}
	}

	return (
		<aside className={`sidebar ${isOpen ? "sidebar-open" : ""}`}>
			<div className="sidebar-header">
				<h2 className="sidebar-title">Chats</h2>
				<button
					className="sidebar-new-btn"
					title="New Chat"
					onClick={handleNewChat}
				>
					<svg
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						strokeWidth="2"
						strokeLinecap="round"
						strokeLinejoin="round"
					>
						<line x1="12" y1="5" x2="12" y2="19" />
						<line x1="5" y1="12" x2="19" y2="12" />
					</svg>
				</button>
			</div>

			<div className="sidebar-search">
				<svg
					className="sidebar-search-icon"
					viewBox="0 0 24 24"
					fill="none"
					stroke="currentColor"
					strokeWidth="2"
					strokeLinecap="round"
					strokeLinejoin="round"
				>
					<circle cx="11" cy="11" r="8" />
					<line x1="21" y1="21" x2="16.65" y2="16.65" />
				</svg>
				<input
					type="text"
					className="sidebar-search-input"
					placeholder="Search conversations..."
					value={searchQuery}
					onChange={(e) => setSearchQuery(e.target.value)}
				/>
			</div>

			<div className="sidebar-list">
				{filteredList?.map((thread) => {
					return (
						<button
							className={`sidebar-item  ${thread.id === activeId ? "active" : ""}`}
							key={thread.id}
							onClick={() => handleSelectThread(thread.id)}
						>
							<svg
								className="sidebar-item-icon"
								viewBox="0 0 24 24"
								fill="none"
								stroke="currentColor"
								strokeWidth="1.5"
								strokeLinecap="round"
								strokeLinejoin="round"
							>
								<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z" />
							</svg>
							<div className="sidebar-item-content">
								<span className="sidebar-item-title">{thread.message}</span>
								<span className="sidebar-item-preview">{thread.aiMessage}</span>
							</div>
							<button
								className="sidebar-item-delete"
								title="Delete thread"
								onClick={(e) => handleDeleteThread(e, thread.id)}
							>
								<svg
									viewBox="0 0 24 24"
									fill="none"
									stroke="currentColor"
									strokeWidth="2"
									strokeLinecap="round"
									strokeLinejoin="round"
								>
									<polyline points="3 6 5 6 21 6" />
									<path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6" />
									<path d="M10 11v6" />
									<path d="M14 11v6" />
									<path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2" />
								</svg>
							</button>
						</button>
					);
				})}
			</div>
		</aside>
	);
}
