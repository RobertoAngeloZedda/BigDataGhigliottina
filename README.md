# BigDataGhigliottina

"La Ghigliottina" is a linguistic game proposed as the final challenge in the extremely popular TV game show "L'eredità" broadcasted on the italian channel "RAI" for over 20 years now.

At the end of each episode, after being given five clues, the contestant has one minute to find the solution: a word that logically connects to each of the received clues. If they guess correctly, they win the prize money; otherwise, they can participate in the next episode as the "reigning champion" without winning anything.

The goal of this project is to create a Telegram bot that, given the 5 clues, is capable of solving this final part of the game.

The project gathers its knowledge from a set of different corpuses. In this repository you will find all the files needed to execute our bot and almost all the files needed to replicate the project.
The two corpora used to represent the Italian language, the larger ones, are not included in this directory since the storing space is limited. In order to correcly set-up the Telegram Bot you are supposed to download them:
Paisà: <https://www.corpusitaliano.it/>
The Italian Wiki Dump: <https://dumps.wikimedia.org/itwiki/>

## Usage

1. Clone the repository
2. Install all the required python modules

    ``` txt
    pip install -r requirements.txt
    ```

3. Increase maxiumum heap size of JVM, open the **/etc/profile** file and add this line to the beginning of the file

    ``` py
    export _JAVA_OPTIONS=-Xmx8192m
    ```

4. Dowload the Paisà and Wikipedia dumps
5. Clean the Wikipedia dump using  <https://github.com/attardi/wikiextractor>
6. Use to **fix_paisa.py** and **fix_wiki.py** to format them, then add them to the corpus folder.
7. Create the model

    ``` py
    python preprocessing.py
    ```

8. Enter the Telegram API Key into **telegrambot.py** and launch the chatbot

    ``` py
    python telegrambot.py
    ```
