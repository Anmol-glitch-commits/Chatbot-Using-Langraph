export default function Navbar({ onToggleSidebar }) {
	return (
		<nav className="navbar">
			<div className="navbar-left">
				<button
					className="hamburger-btn"
					onClick={onToggleSidebar}
					aria-label="Toggle sidebar"
				>
					<svg
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						strokeWidth="2"
						strokeLinecap="round"
						strokeLinejoin="round"
					>
						<line x1="3" y1="6" x2="21" y2="6" />
						<line x1="3" y1="12" x2="21" y2="12" />
						<line x1="3" y1="18" x2="21" y2="18" />
					</svg>
				</button>
				<div className="navbar-brand">
					<svg className="navbar-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
						<path d="M12 2a7 7 0 0 1 7 7c0 2.38-1.19 4.47-3 5.74V17a2 2 0 0 1-2 2H10a2 2 0 0 1-2-2v-2.26C6.19 13.47 5 11.38 5 9a7 7 0 0 1 7-7z" />
						<line x1="10" y1="22" x2="14" y2="22" />
						<line x1="12" y1="19" x2="12" y2="22" />
					</svg>
					<h1 className="navbar-title">AI Assistant</h1>
				</div>
			</div>
			<div className="navbar-status">
				<span className="status-dot"></span>
				<span className="status-text">Online</span>
			</div>
		</nav>
	);
}
