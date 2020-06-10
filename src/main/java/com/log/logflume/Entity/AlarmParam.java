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

    public void alarm(String message){
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        jedis.hset("keyWordAlarm",""+System.currentTimeMillis(),message);
        jedis.close();
    }
}
