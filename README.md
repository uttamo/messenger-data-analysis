# Messenger Data Analysis
Analyse Messenger data JSON exports from Facebook.

## Usage
Export Facebook messages data as JSON
```python
>>> import os
>>> chat_dir = 'data/messages/inbox/GroupChatName_eoK-g8wEea'
>>> os.listdir(chat_dir)
['gifs', 'videos', 'files', 'photos', 'message_1.json']
```
##### Initialise analyser for a particular chat giving the path to the chat directory (the chat directory must contain a file called `message_1.json`)
```python
>>> from analyse import MessengerData
>>> chat = MessengerData(chat_dir)
>>> chat
<MessengerData for 'GroupChatName_eoK-g8wEea' (type=Group, rows=9864)>
```

##### Dataframe of messages
```python
chat.messages_df()
```
Columns: ['sender_name', 'type', 'content', 'call_duration', 'time']

##### Message count by sender
```python
chat.get_no_of_messages_by_sender()
```