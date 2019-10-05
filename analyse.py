import os
import os.path
import json
import logging

import pandas as pd
from ftfy import fix_text

logging.getLogger().setLevel(logging.DEBUG)

DEFAULT_INBOX_PATH = os.path.join('data', 'messages', 'inbox')


class MessengerData:
    def __init__(self, chat_dir_path: str):
        self.chat_id = os.path.basename(chat_dir_path)
        messages_file = self._load_messages_file(chat_dir_path)
        self.df = self._load_messages_df(messages_file)
        self.type = self._determine_chat_type(messages_file['thread_type'])  # 'RegularGroup', 'Regular'
        self.chat_title = fix_text(messages_file['title'])

    def _load_messages_file(self, chat_path: str) -> dict:
        logging.info(f"Loading messages file '{chat_path}'")
        assert os.path.exists(chat_path)
        json_files = [fn for fn in os.listdir(chat_path) if fn.endswith('.json')]
        assert len(json_files) == 1, 'There should be 1 JSON file'
        json_file = json_files[0]
        assert json_file == 'message_1.json'
        messages_file_path = os.path.join(chat_path, json_file)
        with open(messages_file_path, 'r') as inp:
            json_file = json.load(inp)
        return json_file

    def _load_messages_df(self, json_file: dict) -> pd.DataFrame:
        logging.info(f"Loading messages dataframe")
        messages_raw_data = json_file['messages']
        df = pd.DataFrame(messages_raw_data)
        # Convert UNIX timestamps (ms) to proper datetimes
        df['timestamp_s'] = df['timestamp_ms'] / 1000
        df['time'] = pd.to_datetime(df['timestamp_s'], unit='s')

        # Sort in chronological order
        df = df.sort_values('time', ascending=True).reset_index().drop('index', axis=1)

        # Only interested in 'normal' messages
        # df = df[df['type'] == 'Generic']

        # Fix incorrect text decoding
        df['content'] = df['content'].fillna('').apply(fix_text)

        final_columns = [c for c in ['sender_name', 'type', 'content', 'call_duration', 'time'] if c in df.columns]

        df = df[final_columns]
        return df

    def _determine_chat_type(self, thread_type: str) -> str:
        chat_types = {'Regular': 'Regular', 'RegularGroup': 'Group'}
        if thread_type not in chat_types:
            raise NotImplementedError(f"Unrecognised thread type '{thread_type}'")
        return chat_types[thread_type]

    def __repr__(self):
        return "<MessengerData for '{}' (type={}, rows={})>".format(self.chat_id, self.type, len(self))

    def __len__(self):
        return len(self.df)

    def __gt__(self, other):
        return len(self) > len(other)

    def __ge__(self, other):
        return len(self) >= len(other)

    def _get_specific_msg_type_df(self, message_type: str) -> pd.DataFrame:
        return self.df[self.df['type'] == message_type]

    def calls_df(self):
        return self._get_specific_msg_type_df('Call')

    def messages_df(self):
        return self._get_specific_msg_type_df('Generic')

    def get_no_of_messages_by_sender(self):
        msgs_by_sender = self.df.groupby('sender_name').size().to_frame('msg_count')
        msgs_by_sender['msg_count_pct'] = 100 * msgs_by_sender['msg_count'] / msgs_by_sender['msg_count'].sum()
        return msgs_by_sender.sort_values('msg_count', ascending=False)

    def _get_message_count_by_period(self, period: str):
        grouped_by_period = self.df.groupby(pd.Grouper(key='time', freq=period)).size()
        grouped_by_period.name = self.chat_title
        return grouped_by_period

    def get_message_count_by_year(self):
        return self._get_message_count_by_period('Y')

    def get_message_count_by_month(self):
        return self._get_message_count_by_period('M')

    def get_message_count_by_day(self):
        return self._get_message_count_by_period('D')


class MessengerAnalyser:
    def __init__(self, *chats):
        assert all(type(i) is MessengerData for i in chats), 'Args for MessengerAnalyser must be of type MessengerData'
        self.chats = chats
        self.daily_message_counts_df = self.get_message_counts()

    def get_message_counts(self) -> pd.DataFrame:
        individual_daily_message_counts = []
        for chat in self.chats:
            daily_msg_count = chat.get_message_count_by_day()
            individual_daily_message_counts.append(daily_msg_count)
        message_counts = pd.concat(individual_daily_message_counts, axis=1).fillna(0)
        return message_counts

    def plot_message_count(self):
        cumulative_daily_message_counts = self.daily_message_counts_df.cumsum()
        ax = cumulative_daily_message_counts.plot(figsize=(20, 10), lw=3)
        ax.set_ylabel('Total message count')
