# BigDataGhigliottina

La Ghigliottina" is a linguistic game proposed as the final challenge in the extremely popular TV game show "L'eredità" broadcasted on the italian channel "RAI" for over 20 years now.

At the end of each episode, after being given five clues, the contestant has one minute to find the solution: a word that logically connects to each of the received clues. If they guess correctly, they win the prize money; otherwise, they can participate in the next episode as the "reigning champion" without winning anything.

The goal of this project is to create a Telegram bot that, given the 5 clues, is capable of solving this final part of the game.

The project gathers its knowledge from a set of different corpuses. In this repository you will find all the files needed to execute our bot and almost all the files needed to replicate the project.
The two corpora used to represent the Italian language, the larger ones, are not included in this directory since the storing space is limited. In order to correcly set-up the Telegram Bot you are supposed to download them:
Paisà: https://www.corpusitaliano.it/
The Italian Wiki Dump: https://dumps.wikimedia.org/itwiki/
Use the following tool to parse the Wiki Dump: https://github.com/attardi/wikiextractor


Finally, in order to run the bot, you'll have to create ad your telegram key to the file named "telegrambot.py".

## Usage

1. Clone the repository
2. Install all the required python modules

``` txt
pip install -r requirements.txt
```
