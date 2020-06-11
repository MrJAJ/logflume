package com.log.logflume.Entity;

import redis.clients.jedis.Jedis;

import java.io.Serializable;

public class AlarmParam implements Serializable {
    private String keyWord;
    private int errorThreshold;
    private float errorrateThreshold;

    public AlarmParam() {
    }

    public AlarmParam(String keyWord, int errorThreshold, float errorrateThreshold) {
        this.keyWord = keyWord;
        this.errorThreshold = errorThreshold;
        this.errorrateThreshold = errorrateThreshold;
    }

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public int getErrorThreshold() {
        return errorThreshold;
    }

    public void setErrorThreshold(int errorThreshold) {
        this.errorThreshold = errorThreshold;
    }

    public float getErrorrateThreshold() {
        return errorrateThreshold;
    }

    public void setErrorrateThreshold(float errorrateThreshold) {
        this.errorrateThreshold = errorrateThreshold;
    }

    public void keyWordAlarm(String message){
        Jedis jedis = new Jedis("133.133.135.26", 6379);
        jedis.hset("Anomy","1\t"+System.currentTimeMillis(),message);
        jedis.close();
    }

    public void thresholdAlarm(String message){
        Jedis jedis = new Jedis("133.133.135.26", 6379);
        jedis.hset("Anomy","2\t"+System.currentTimeMillis(),message);
        jedis.close();
    }

    public void errorrateThreshold(String message){
        Jedis jedis = new Jedis("133.133.135.26", 6379);
        jedis.hset("Anomy","2\t"+System.currentTimeMillis(),message);
        jedis.close();
    }
//package com.log.logflume.Entity

//    rule "rule2"
//    salience 1
//    when
//    a:AlarmParam(a.errorThreshold == 0)
//    then
//    a.alarm(a.getErrorThreshold()+"异常");
//    end
}
