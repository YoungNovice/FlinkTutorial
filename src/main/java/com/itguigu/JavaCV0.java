package com.itguigu;

import org.bytedeco.ffmpeg.global.avutil;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.Java2DFrameConverter;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public class JavaCV0 {

    public static void main(String[] args) throws IOException {
        String fileUrl1 = "/Users/yangxuan/Desktop/1.mp4";
        String fileUrl = "https://realty-commons.dtyunxi.cn/yxy-test-filesystem/yxy-sit/assets/69cd54e0-afd2-11ec-87a9-33805619cd6e.mp4";
        org.bytedeco.ffmpeg.global.avutil.av_log_set_level(org.bytedeco.ffmpeg.global.avutil.AV_LOG_ERROR);
        URL url = new URL(fileUrl);
        URLConnection connection = url.openConnection();
        InputStream inputStream = connection.getInputStream();
        FFmpegFrameGrabber fFmpegFrameGrabber = new FFmpegFrameGrabber(inputStream);

        fFmpegFrameGrabber.start();
        Frame frame = null;
        //获取视频总帧数
        int frames = fFmpegFrameGrabber.getLengthInFrames();
        int i = 0;
        while (i <= frames) {
            frame = fFmpegFrameGrabber.grabImage();
            //截取第10帧
            if (frame != null && i == 10) {
                String fileName = "/Users/yangxuan/Desktop/" + System.currentTimeMillis() + ".jpg";
                File file = new File(fileName);
                //创建BufferedImage对象
                Java2DFrameConverter converter = new Java2DFrameConverter();
                BufferedImage bufferedImage = converter.getBufferedImage(frame);
                ImageIO.write(bufferedImage, "jpg", file);
                break;
            }
            i++;
        }
        fFmpegFrameGrabber.stop();
        fFmpegFrameGrabber.close();
    }
}
