#! /usr/bin/python3 
# -*- coding: utf-8 -*-
# aihub link: https://aihub.or.kr/aihubdata/data/view.do?currMenu=115&topMenu=100&dataSetSn=505
# model link: https://drive.google.com/drive/u/0/folders/1u95BbGAoUogn38_eMkvFHJHjlgnUqLBe
## essay aihub link: https://aihub.or.kr/aihubdata/data/view.do?currMenu=115&topMenu=100&dataSetSn=545

'''
	Kaldi 학습을 위한 데이터 전처리 및 테스트셋 생성
'''

import json
import os
import subprocess
import sys
from random import random
from time import time

# User configs
IN = sys.argv[1] # 원본 데이터 (e.g. /mnt/raid/sypaik_disk/NIA/3차_최종)
OUT = sys.argv[2] # Kaldi 포맷 학습 데이터
OUT_TESTSET = sys.argv[3] # 테스트셋
OUT_CORPUS = sys.argv[4] # Kaldi 포맷 학습 데이터 (텍스트)

# Configs
AUDIO_DIR = '1. 원천데이터' # 오디오 파일이 위치한 폴더명
EXT = ["json"]
testset_ratio = 0.05

def getAudioDuration(audio_file):
	eeturn subprocess.check_output('soxi -D \'' + audio_file + '\'', shell=True).decode('utf-8').strip()

def validateInOut(IN, OUT):
	if(not os.path.isdir(IN)):
		print('ERROR: IN')
		sys.exit(0)
	if(os.path.isdir(OUT)):
		print('ERROR: OUT')
		sys.exit(0)
	print('in/out checked valid...')

def getJsonList(IN):
	li = []
	num = 0
	for dirpath, dirnames, filenames in os.walk(IN):
		for filename in [f for f in filenames if f.endswith(tuple(EXT))]:
			curr_json = os.path.join(dirpath, filename)
			num += 1
			print(f'Collecting list of json files... {[num]} {curr_json}')
			li.append(curr_json)
	return li

def isTestsetUnique(testset_ratio, sentence_id):
	# 자유발화: 226~300
	if(int(sentence_id) >= 226 and int(sentence_id) <= 300):
		if(random() < testset_ratio):
			return True
		else:
			return False
	else:
		return False

def walkAndCreateDataset(json_list, OUT, OUT_CORPUS, OUT_TESTSET):
	time0 = time()
	corpus_text = ''
	broken_list = []
	testset_count = 0
	
	train_sentences = 'debug_train_sentences.txt'
	test_sentences = 'debug_test_sentences.txt'
	if(os.path.exists(train_sentences)):
		subprocess.call(f'rm {train_sentences}', shell=True)
	if(os.path.exists(test_sentences)):
		subprocess.call(f'rm {test_sentences}', shell=True)

	for i in range(len(json_list)):
		try:
			with open(json_list[i], encoding='utf-8-sig') as json_file:
				json_data = json.load(json_file)
				json_file = json_list[i]

				parent_dir = os.path.basename(os.path.dirname(json_file))
				grandparent_dir = os.path.basename(os.path.dirname(os.path.dirname(json_file)))
				root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(json_file))))
				audio_file = os.path.join(root_dir, AUDIO_DIR, grandparent_dir, parent_dir, json_data['fileName'])

				speaker_id = json_data['SpeakerID']
				skill = json_data['skill_info']['selfAssessment']
				speaker_id = speaker_id + '_' + skill
				sentence_id = json_data['file_info']['sentenceID']
				sentence_id = sentence_id[-3:] ### !!!
				if(not json_data['transcription']['ReadingLabelText']):
					text = json_data['transcription']['AnswerLabelText'].strip()
				elif(not json_data['transcription']['AnswerLabelText']):
					text = json_data['transcription']['ReadingLabelText'].strip()
				gender = json_data['basic_info']['gender']
				utt_id = speaker_id + '_' + sentence_id

				if(isTestsetUnique(testset_ratio, sentence_id)):
					testset_count += 1
					if(i % 100 == 0):
						time_now = time()
						print()
						print(f'[{str(i+1)}/{str(len(json_list))}] 소요시간:{round(time_now - time0,2)}초')
						print(f'\t[Testset][{sentence_id}] {text} | [# of Testset: {testset_count}]')
						print()
					dst_dir = os.path.join(OUT_TESTSET, skill)
					dst_audio = os.path.join(dst_dir, utt_id+'.wav')
					dst_text = os.path.join(dst_dir, utt_id+'.txt')
					
					if(not os.path.isdir(dst_dir)):
						cmd = 'mkdir -p ' + dst_dir
						subprocess.call(cmd, shell=True)

					cmd = 'sox \'' + audio_file + '\' -r 16k -b 16 -c 1 ' + dst_audio
					subprocess.call(cmd, shell=True)
					
					with open(dst_text, 'w') as f_text:
						f_text.write(text)
			
					with open(test_sentences, 'a') as fw_test:
						fw_test.write(text + '\n')

					continue

				if(i % 100 == 0):
					time_now = time()
					print(f'[{str(i+1)}/{str(len(json_list))}] 소요시간:{round(time_now - time0,2)}초')
					print(f'[Non-Testset][{sentence_id}] {text} | [# of Testset: {testset_count}]')

				corpus_text += text + '\n'
				with open(train_sentences, 'a') as fw_train:
					fw_train.write(text + '\n')

				audio_duration = getAudioDuration(audio_file)
				
				speaker_dir = os.path.join(OUT, speaker_id)
				subprocess.call('mkdir -p ' + speaker_dir, shell=True)
				
				sentence_dir = os.path.join(speaker_dir, sentence_id)
				subprocess.call('mkdir -p ' + sentence_dir, shell=True)
	
				flac_audio = os.path.join(sentence_dir, utt_id + '.flac')
				kaldi_segments = os.path.join(sentence_dir, 'segments')
				kaldi_spk2gender = os.path.join(sentence_dir, 'spk2gender')
				kaldi_text = os.path.join(sentence_dir, 'text')
				kaldi_utt2dur = os.path.join(sentence_dir, 'utt2dur')
				kaldi_utt2spk = os.path.join(sentence_dir, 'utt2spk')
				kaldi_wav_scp = os.path.join(sentence_dir, 'wav.scp')
	
				cmd = 'sox \'' + audio_file + '\' -r 16k -b 16 -c 1 ' + flac_audio
				subprocess.call(cmd, shell=True)
	
				with open(kaldi_segments, 'w') as f_segments:
					line = utt_id + ' ' + sentence_id + ' 0.0 ' + audio_duration
					f_segments.write(line)
				with open(kaldi_spk2gender, 'w') as f_spk2gender:
					line = speaker_id + ' ' + gender.lower()
					f_spk2gender.write(line)
				with open(kaldi_text, 'w') as f_text:
					line = utt_id + ' ' + text
					f_text.write(line)
				with open(kaldi_utt2dur, 'w') as f_utt2dur:
					line = utt_id + ' ' + audio_duration
					f_utt2dur.write(line)
				with open(kaldi_utt2spk, 'w') as f_utt2spk:
					line = utt_id + ' ' + speaker_id
					f_utt2spk.write(line)
				with open(kaldi_wav_scp, 'w') as f_wav_scp:
					line = utt_id + ' flac -c -d -s ' + flac_audio + ' | '
					f_wav_scp.write(line)


		except:
			print(f'Problem occurred when processing {json_list[i]}')
			broken_list.append(json_list[i])

	print(broken_list)
	print('# of broken data : ' + str(len(broken_list)))

	with open(OUT_CORPUS, 'w') as f_corpus:
		f_corpus.write(corpus_text)


# [Data Preprocessing begins]

if(os.path.isdir(OUT)):
	print(f'{OUT} already exists -> removed')
	subprocess.call('rm -r ' + OUT, shell=True)
if(os.path.isdir(OUT_TESTSET)):
	print(f'{OUT_TESTSET} already exists -> removed')
	subprocess.call('rm -r ' + OUT_TESTSET, shell=True)
validateInOut(IN, OUT)
print(f'{OUT} created')
subprocess.call('mkdir -p ' + OUT, shell=True)
print(f'{OUT_TESTSET} created')
subprocess.call('mkdir -p ' + OUT_TESTSET, shell=True)

json_list = getJsonList(IN)

walkAndCreateDataset(json_list, os.path.abspath(OUT), OUT_CORPUS, os.path.abspath(OUT_TESTSET))
print(OUT)
print(OUT_CORPUS)
print(OUT_TESTSET)

# [Data Preprocessing ends]
