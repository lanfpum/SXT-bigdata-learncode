package lxpsee.top.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * The world always makes way for the dreamer
 * Created by 努力常态化 on 2019/2/19 17:08.
 */
public class DoubleBallTest {
    public static void main(String[] args) {
        int zhu = 2;

//        int[] arrBei = {2, 3, 4, 5};
        int[] arrBei = {5};
        int bei = arrBei[(int) (Math.random() * 5)];
        String beishu = "买 " + bei + " 倍！";

        for (int i = 0; i < zhu; i++) {
            String result = genDoubleBallNO();
            System.out.println(result + " : " + beishu);
        }

    }

    private static String genDoubleBallNO() {
        Set<Integer> redBalls = new HashSet<Integer>();

        while (redBalls.size() < 6) {
            int red = (int) (Math.random() * 33 + 1);
            redBalls.add(red);
        }

        int blueBall = (int) (Math.random() * 16 + 1);
        ArrayList<Integer> redBallList = new ArrayList<Integer>(redBalls);
        Collections.sort(redBallList);
        return "红球：" + redBallList + " 篮球" + blueBall;
    }
}