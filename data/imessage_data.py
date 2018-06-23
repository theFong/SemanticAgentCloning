import sqlite3
import argparse
import os
import errno
import time
import multiprocessing
from multiprocessing import Pool
import tqdm
from pathlib import Path
import re
dir_path = os.path.dirname(os.path.realpath(__file__))

parser = argparse.ArgumentParser()
parser.add_argument("--ids",
                    help="Enter 1 or more message ids in format +1XXXXXXXXXX,...")
parser.add_argument('-t','--test', help='Run test', action="store_true")
args = parser.parse_args()


class iMessageData:
    def __init__(self, ids=[], all=False):
        self.ids = ids
        self.dataset = ''
        self.all = all
        self.filtered_message = ''

    def fromIMessage(self,dbPath= str(Path.home()) + '/Library/Messages/chat.db',  writePath=dir_path+'/datasets'):
        messages = self.getMessagesFromDb(dbPath)
        self.dataset = self.processMessages(messages)
        self.writeDataset(writePath)
        
    def processMessages(self, messages):
        processed_lines = ''
        # put all of one person dialog on a line
        prev_speaker = messages[0][2] #is_from_me
        processed_lines += messages[0][0]
        filter_count = 0
        for m in messages[1:]:
            is_filtered, m = self.filterSanitize(m)
            if is_filtered:
                filter_count += 1
                self.filtered_message += str(m[0]) + '\n'
                continue
            line = ''
            if m[2] != prev_speaker:
                line += '\n'
            else:
                line += ' '
            line += m[0] # add text
            prev_speaker = m[2]

            processed_lines += line
        print('filtered  # messages: ', filter_count)
        return processed_lines

    def getMessagesFromDb(self, path):
        db = sqlite3.connect(path) # make sure to use abs path
        cursor = db.cursor()

        if not self.all:
            sql_id_statement = str(self.ids)[1:-1].replace(',','or id =')
            cursor.execute('''
            select text, date, is_from_me, handle_id
            from message
            inner join
            (select ROWID from handle where id = '''+ sql_id_statement +''') as handle_
            on message.handle_id = handle_.ROWID
            order by date
            ''')
        else:
            cursor.execute('''
            select text, date, is_from_me, handle_id
            from message
            order by date
            ''')
        messages = cursor.fetchall()
        db.close()
        print('retrieved # messages:', len(messages))
        return messages

    # has worse performance, need to profile
    def fromIMessageParallel(self,dbPath=str(Path.home()) + '/Library/Messages/chat.db',  writePath=''):
        messages = self.getMessagesFromDb(dbPath)
        start_time = time.time()
        pool_size = multiprocessing.cpu_count() + 1
        pool = Pool(pool_size)
        process_batch_size = int(len(messages) / pool_size)
        indexes = [-1]
        for i in range(pool_size-1):
            ind = process_batch_size * (i+1)
            prev_speaker = messages[ind][2]
            while messages[ind][2] == prev_speaker:
                prev_speaker = messages[ind][2]
                ind += 1
            indexes.append(ind)
        indexes.append(len(messages)-1)

        batches = [ messages[indexes[p]+1:indexes[p+1]] for p in range(0,pool_size)]
        res = pool.map_async(self.processMessages, batches)
        pool.close()
        pool.join()
        print("--- %s seconds ---" % (time.time() - start_time))

        message_batches_processed = res.get()
        
        self.dataset = '\n'.join(message_batches_processed)
        print(self.dataset[:500])
    
    def filterSanitize(self, message):
        if message[0] == None: # remove null
            return True, message
        text = message[0].replace('\n',' ') # replace new line with ' '
        text = text.lstrip() # remove whitespace begining of string
        text = text.replace(u'\ufffc','') #remove object replacement unicode char

        if len(text) == 0: # remove zero length string
            return True, message
        if self.is_reaction(message[0]): # remove reaction
            return True, message

        tokens = []
        for t in text.split(' '): # split tokens by ''
            if 'http' not in t: # check if token is not link
                tokens.append(t)
        if len(tokens) == 0: # if only a link remove
            return True, message
        text = ' '.join(tokens) # join string without link
        message = (text, message[1], message[2])
        return False, message

    def is_reaction(self, text):
        # match reaction message
        # need to check if it will work with newer OS
        return re.match('(Loved|Laughed|Liked|Disliked|Emphasized|Questioned) (at|an)?( )?([\"\“].*[\"\”]|image)', text)

    def writeDataset(self, path=dir_path+'/datasets'):
        if not self.all:
            filename = path+'/im_'+str(self.ids)+'.txt'
            filtered_filename = path+'/im_'+'filtered'+str(self.ids)+'.txt'
        else:
            filename = path+'/entire_message_data.txt'
            filtered_filename = path+'/filtered_entire_message_data.txt'

        if not os.path.exists(os.path.dirname(filename)):
            try:
                os.makedirs(os.path.dirname(filename))
            except OSError as exc: # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

        with open(filename, "w") as f:
            f.write(self.dataset)
        with open(filtered_filename, 'w') as f:
            f.write(self.filtered_message)

def main():
    ids = args.ids.split(',')
    for i in ids:
        imd = iMessageData([i])
        imd.fromIMessage()
        imd.writeDataset()

def test():
    imd = iMessageData(['+1..........']) # change with phonenumer/id in db
    imd.fromIMessage()
    imd.writeDataset()

if __name__ == '__main__':
    if args.test:
        test()
    else:
        main()
