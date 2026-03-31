import axios from "axios";

const api = axios.create({
	baseURL: "https://chatbot-backend-fastapi-production.up.railway.app/api",
});

export async function sendMessage(message, thread_id) {
	const res = await api.post("/chat", {
		message: message,
		thread_id: thread_id,
	});

	return res.data;
}

export async function createThread() {
	const res = await api.post("/threads");
	return res.data;
}

export async function getThreads() {
	const res = await api.get("/threads");
	return res.data;
}

export async function getThreadMessages(thread_id) {
	const res = await api.get(`/threads/${thread_id}/messages`);
	return res.data;
}

export async function uploadPdf(file, thread_id) {
	const formdata = new FormData();
	formdata.append("file", file);
	formdata.append("thread_id", thread_id);

	const res = await api.post("/upload_pdf", formdata);

	return res.data;
}

export async function deleteThread(thread_id) {
	const res = await api.delete(`/threads/${thread_id}`);
	return res.data;
}
