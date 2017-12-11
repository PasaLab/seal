# --** word split use jieba parallel -- *
# -----Yang Wenjia --------
# -----2016.12.16---------
import time
import sys
reload(sys)
sys.path.append("/home/ywj/jieba-0.38")
import jieba

sys.setdefaultencoding('utf8')
jieba.enable_parallel(20)

#filename = sys.argv[1]
ta = time.time()
content = open("/home/ywj/wordsplit/UNv1.0.en-zh.zh", "r").read()

t1 = time.time()

words = " ".join(jieba.cut(content))

t2 = time.time()
tm_cost=t2-t1

res = open("/home/ywj/wordsplit/UNPC.en-zh.zh", "w")

res.write(words.encode('utf-8'))
tb = time.time()
print('speed %s bytes/second' % (len(content)/tm_cost))

print('tatal cost %s time ' % (tb-ta)) 
