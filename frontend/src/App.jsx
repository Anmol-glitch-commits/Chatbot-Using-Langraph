import Navbar from "./components/Navbar";
import Sidebar from "./components/Sidebar";
import MainChat from "./components/MainChat";
import InputBox from "./components/InputBox";
import { useEffect, useState, useCallback } from "react";
import { getThreads, getThreadMessages } from "./api/api";

export default function App() {
  const [threadId, setThreadId] = useState("");
  const [messages, setMessages] = useState([]);
  const [message, setMessage] = useState("");
  const [threadList, setThreadList] = useState([]);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  const toggleSidebar = useCallback(() => setSidebarOpen((prev) => !prev), []);
  const closeSidebar = useCallback(() => setSidebarOpen(false), []);

  useEffect(() => {
    async function getAllThreads() {
      const res = await getThreads();

      //list of threads
      const threads = res.threads;

      const threadData = await Promise.all(
        threads.map(async (thread_id) => {
          const messages = await getThreadMessages(thread_id);

          const firstMessage = messages.messages[0]?.content || "New Chat";
          const aiMessage = messages.messages[1]?.content || "";

          return {
            id: thread_id,
            message: firstMessage,
            aiMessage: aiMessage,
          };
        }),
      );

      setThreadList(threadData);
    }

    getAllThreads();
  }, []);

  return (
    <div className="app-layout">
      {sidebarOpen && (
        <div className="sidebar-overlay" onClick={closeSidebar} />
      )}
      <Sidebar
        threadList={threadList}
        setThreadId={setThreadId}
        activeId={threadId}
        setMessages={setMessages}
        setThreadList={setThreadList}
        isOpen={sidebarOpen}
        onClose={closeSidebar}
      />
      <div className="app-main">
        <Navbar onToggleSidebar={toggleSidebar} />
        <MainChat
          activeId={threadId}
          messages={messages}
          setMessages={setMessages}
          isLoading={isLoading}
        />
        <InputBox
          message={message}
          setMessage={setMessage}
          activeId={threadId}
          setThreadId={setThreadId}
          messages={messages}
          setMessages={setMessages}
          setThreadList={setThreadList}
          isLoading={isLoading}
          setIsLoading={setIsLoading}
        />
      </div>
    </div>
  );
}
