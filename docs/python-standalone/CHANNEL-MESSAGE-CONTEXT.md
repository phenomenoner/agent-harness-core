# Channel message context and media input

The foreground runtime keeps the current user message separate from quoted and
forwarded context. Telegram selected quotes take precedence over the replied-to
message text or caption. Discord replies retain available same-channel reference
text. Missing references are represented as unavailable; the runtime does not
invent the original message or fetch another channel's history.

Context is bounded and stored with the original operation and route. It is
projected as data in the model input and in bounded conversation history. Quoted
slash commands are not commands. Existing submissions without context remain
readable, and enrichment does not change an event's deduplication identity.

When file reception is enabled, Telegram photos and documents and Discord
attachments enter durable staging before download. Downloads verify the exact
route, declared byte count and permitted provider endpoint. Up to eight files
and one MiB per file are accepted. Imported supported images are supplied as
native image input rather than base64 text in a tool result. Native image input
has a separate aggregate byte budget; excluded images receive an explicit
model-visible limitation. Other imported files remain accessible through the
artifact tools.

An import seals its ordered artifact selection to the exact operation and event.
Changing that selection invalidates the import even when a replacement artifact
has the same filename, size and media type. Older imports without this seal remain
readable, but are not supplied as native images; the agent is told to request a
fresh upload rather than claim to see them.

Unsupported media, disabled file reception and invalid or oversized file
metadata produce a deterministic channel notice without starting a model turn.
An import failure is retained and receives one durable notice; it is not
automatically replayed after restart. Database, route and identity failures are
not silently treated as malformed attachments.

Messages carrying quoted context or files are kept intact for the next turn
while work is active. They are not reduced to text-only steering. Ordinary text
continues to follow the configured queue/steer preference. `/new` holds pending
file imports as well as queued text. `/stop` preserves conversation context.

Telegram albums use bounded buffering and atomic inbox staging, so a sealed album becomes one conversation task with its grouped files. Topic identifiers remain metadata and do not create separate topic sessions. Message
edits do not rerun a prior operation. Speech transcription, video understanding,
sticker interpretation and automatic thread discovery are not provided by this
input layer.
