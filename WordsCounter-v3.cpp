/*********************************************************
* Author:     Pihai Sun                                  *
* University: Qingdao University                         *
* College:    College of Computer Science and Technology *
*********************************************************/

#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <thread>
#include <pthread.h>
#include <cstring>
#include <queue>
#include <vector>
#include <fstream>
#include <sstream>
#include <atomic>
#include <iomanip>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <chrono>

using namespace std;
using namespace std::chrono;

typedef unordered_map<string, int> WordMap;
typedef vector<string> StrVec;
typedef queue<string> StrQueue;
typedef priority_queue<pair<int, string>> StrNum;

/**
 * @staResult: The mapping of words to their numbers
 * @WorkQueue: Hold the lines to be worked on by programs
 * @pathQueue: The path of the file need to be counted
 * @wordsRank: Sort by number of word occurrences
 * @maxQSize:  Maximum number of lines in work queue
 * @queStock:  Number of string lines in the work queue
 * @printNum:  Output 'printNum' words that appear the most
*/
WordMap staResult;
StrQueue workQueue, pathQueue;
StrNum wordsRank;
atomic<int> producerNum = ATOMIC_VAR_INIT(0);
const size_t maxQSize = 10000;
const int printNum = 10;
const std::string instruct =
    "Please entire the path of directory or path, or type exit to quit";

/**
 * @pathMutex:  Mutex for pathQueue
 * @queueMutex: Mutually exclusive access to the work queue
 * @mutexMap:   Mutex for staResult
 * @condQueue:  condition variavle for workQueue -- avoid spin lock
*/
pthread_mutex_t pathMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t queueMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexMap = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t condQueue = PTHREAD_COND_INITIALIZER;

/**
 * Get all txt files under the given path
 * @param path
 * @param pathQueue
*/
void getFiles(string path)
{
    struct stat fSta;
    stat(path.c_str(), &fSta);
    if (S_ISDIR(fSta.st_mode))
    {
        DIR *pDir = opendir(path.c_str());
        struct dirent *ptr = NULL;
        // Access the files in this directory in turn
        while ((ptr = readdir(pDir)) != 0)
        {
            // Recursively enter subfolders
            // ! Must judge whether the file name starts with '.'
            if (ptr->d_type == DT_DIR && ptr->d_name[0] != '.')
            {
                getFiles(path + "/" + ptr->d_name);
                continue;
            }

            string fName = ptr->d_name;
            // Only fetch files of TXT type
            if (fName.find(".txt") != string::npos)
            {
                pathQueue.push(path + "/" + ptr->d_name);
            }
        }
        closedir(pDir);
    }
    else if (S_ISREG(fSta.st_mode) && path.find(".txt") != string::npos)
    {
        // The path is a TXT file
        pathQueue.push(path);
    }
    else
    {
        puts("Invalid path or no TXT file under the path!\n");
    }
}

/**
 * return true if pathQueue is empty
*/
bool isEmptyPath()
{
    bool res;
    pthread_mutex_lock(&pathMutex);
    res = pathQueue.empty();
    pthread_mutex_unlock(&pathMutex);
    return res;
}

/**
 * Read the file in and add each line to the work queue for the producer
 * @param fp The file path
*/
void producer(const string fp)
{

    ifstream fileStream(fp);
    string line;

    while (getline(fileStream, line))
    {
        pthread_mutex_lock(&queueMutex);
        while (workQueue.size() >= maxQSize)
        {
            // Waiting for the work queue is not full
            pthread_cond_wait(&condQueue, &queueMutex);
        }

        // Add the line to end of work queue
        workQueue.push(line);

        // Wake up a waiting thread
        pthread_cond_signal(&condQueue);
        pthread_mutex_unlock(&queueMutex);
    }
}

/**
 * Send the file path in pathQueue to the producer
 * @param pathQueue
*/
void *launchProducer(void *args)
{
    producerNum++;
    string fp;
    while (!isEmptyPath())
    {
        // ! Use trylock instead of lock to avoid blocking forever
        int getLockStu = pthread_mutex_trylock(&pathMutex);
        if (getLockStu != 0)
        {
            // Didn't get the lock
            continue;
        }

        fp = pathQueue.front();
        pathQueue.pop();

        pthread_mutex_unlock(&pathMutex);
        producer(fp);
    }
    producerNum--;
    return NULL;
}

/**
 * Convert punctuation in a line into spaces
 * @param:  line
 * @return: A line of characters without punctuation
*/
string cleanPunct(const string &line)
{
    string str = line;
    replace_if(str.begin(), str.end(), ::ispunct, ' ');
    return str;
}

/**
 * Make a word lowercase
 * @param word
 * @return A lowercased word
*/
void toLowerCase(string &str)
{
    transform(str.begin(), str.end(), str.begin(), ::tolower);
}

/**
 * return true if work queue is empty
*/
bool isEmptyWork()
{
    bool res;
    pthread_mutex_lock(&queueMutex);
    res = workQueue.empty();
    pthread_mutex_unlock(&queueMutex);
    return res;
}

/**
 * Count the words in this line
 * @param staResult
 * @param line
 * ! Can not pass reference type parameters to the function,
 * ! because the 'line' will be modified in the consumer
*/
void wordCount(string line)
{
    cleanPunct(line);
    istringstream is(line);
    string word;
    while (is >> word)
    {
        toLowerCase(word);
        // Exclusive access to staREsult
        pthread_mutex_lock(&mutexMap);
        staResult[word]++;
        pthread_mutex_unlock(&mutexMap);
    }
}

/**
 * Take out the string stored in the work queue by the producer
 * and send it to the Counter function
*/
void *consumer(void *args)
{
    string line;
    // loop while producer is working or queue is not empty
    while (producerNum > 0 || !isEmptyWork())
    {
        pthread_mutex_lock(&queueMutex);
        while (producerNum > 0 && workQueue.empty())
        {
            // Wait for the producer to stop reading or the work queue is not empty
            pthread_cond_wait(&condQueue, &queueMutex);
        }

        if (!workQueue.empty())
        {
            // Only take strings when the work queue is not empty
            line = workQueue.front();
            workQueue.pop();
        }

        pthread_cond_signal(&condQueue); // Wake up a waiting thread
        pthread_mutex_unlock(&queueMutex);

        // Count words in this line
        wordCount(line);
    }
    return NULL;
}

/**
 * Sort the word frequency in staResult in descending order
 * @param staResult
 * @param wordsRank
*/
void sortWords()
{
    auto iter = staResult.begin();
    while (iter != staResult.end())
    {
        wordsRank.push(make_pair(iter->second, iter->first));
        iter++;
    }
}

/**
 * Output the 'printNum' most frequent words
 * @param wordsRank
 * @param printNum 
*/
void printHotWords()
{
    // Print title
    printf("%-10s%-20s%-10s\n", "RANK", "KEY WORDS", "FREQUENCY");
    for (int i = 1; i <= printNum && !wordsRank.empty(); i++)
    {
        cout << setiosflags(ios::left)
             << setw(10) << i << setw(20) << wordsRank.top().second
             << setw(10) << wordsRank.top().first << endl;
        wordsRank.pop();
    }
}

/**
 * Launch all the threads needed
 * @param threadGroup
 * @param fp
 * @param threadGroup
 * @param threads
 */
void threadLauncher(std::vector<pthread_t *> &threadGroup, int cores, string &path)
{
    getFiles(path);
    for (int i = 0; i < cores / 2; i++)
    {
        threadGroup.push_back(new pthread_t);
        pthread_create(threadGroup[i], NULL, launchProducer, NULL);
    }

    for (int i = cores / 2; i < cores; i++)
    {
        threadGroup.push_back(new pthread_t);
        pthread_create(threadGroup[i], NULL, consumer, NULL);
    }

    for (int i = 0; i < threadGroup.size(); i++)
    {
        pthread_join(*threadGroup[i], NULL);
        free(threadGroup[i]);
        threadGroup[i] = NULL;
    }

    sortWords();
    printHotWords();
}

/**
 * Initialize variables
*/
void initVariable()
{
    staResult.clear();
    while (!wordsRank.empty())
        wordsRank.pop();
}

int main()
{
    size_t cores = std::thread::hardware_concurrency();
    cout << cores << " concurrent threads are supported.\n";

    string path;
    cout << instruct << endl;
    while (cin >> path)
    {
        if (path == "exit" || path == "quit")
        {
            return 0;
        }

        auto start = high_resolution_clock::now();

        // launch threads to process
        std::vector<pthread_t *> threadGroup;
        threadLauncher(threadGroup, cores, path);

        auto stop = high_resolution_clock::now();
        auto duration = duration_cast<milliseconds>(stop - start);
        cout << "Time taken by function: "
             << duration.count() << " milliseconds" << endl;

        cout << endl
             << instruct << endl;
        initVariable();
    }
}