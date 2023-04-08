#  pip install all the below imported files
import speech_recognition as sr
import pyttsx3 
import pywhatkit
import datetime 
import wikipedia
import pyjokes


listener= sr.Recognizer()
#  listener variable will have the recognized voice
engine= pyttsx3.init()
#  engine variable will get python text to speecha initilized 
voice= engine.getProperty('voices')
#  in voice we are having the voice from speech to text and in below line we have set our engine voice to voice[1]
#  female voice
engine.setProperty('voice',voice[1].id)

def talk(text):
    engine.say(text)
    engine.runAndWait()
    #  this function says the text provided and run and wait

def take_command():
    try:
        with sr.Microphone() as source:
            print('listening......')
            voice= listener.listen(source)
            # listens the voice and recognize the voice 
            command=listener.recognize_google(voice)
            command= command.lower()
            if 'alexa' in command:
                command= command.replace('alexa','')
                #  if alexa word found in our command while printing the command given the alexa will be replaced
                print(command)

    except:
        pass
    return command

#  in this function we have defined what alexa should perform for the given set of words
def run_alexa():
    command= take_command()
    print(command)
    if 'play' in command:
        song = command.replace('play','')
        talk('playing'+song)
        pywhatkit.playonyt(song)

    elif 'time' in command:
        time=datetime.datetime.now().strftime('%H:%M')
        print(time)
        talk('current time is'+time)
    
    elif 'who the heck is' in command:
        person= command.replace('who the heck is','')
        info = wikipedia.summary(person,1)
        print(info)
        talk(info)
    
    elif 'date' in command:
        talk('sorry, mummy allow nhi kregi')
    elif 'are u single' in command:
        talk('no i am in relationship with wifi')
    elif 'joke' in command:
        talk(pyjokes.get_joke())
    else:
        talk('please say it again and clear')


run_alexa()
