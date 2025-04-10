# 🤖 doc-savvy RAG Agent

An AI-powered assistant that reads documents from a SharePoint folder and answers questions, gives recommendations, and generates templates/playbooks — using Retrieval-Augmented Generation (RAG).

---

## 📦 What It Does

This AI agent can:

- Connect to a SharePoint folder and ingest documents
- Use embeddings to understand document content
- Answer questions and generate recommendations based on the files
- Create structured outputs like templates, checklists, and playbooks

---

## 🧠 Tech Stack

- Python 🐍
- LLM
- LangChain for agent orchestration
- Azure AD + Microsoft Graph API for SharePoint access
- Vector DB for retrieval

---

## 🚀 Getting Started

### 1. Clone the repo

```bash
git clone https://github.com/teofizzy/doc-savvy.git
cd doc-savvy
