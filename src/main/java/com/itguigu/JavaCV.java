package com.itguigu;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;
import org.bytedeco.javacv.Java2DFrameConverter;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

public class JavaCV {

    /**
     * 获取指定帧数的封面图片
     *
     * @param frameNum 帧数
     * @param filePath 文件所在路径
     */
    public static BufferedImage getBufferedImageByFrame(int frameNum, String filePath) throws IOException {
        FFmpegFrameGrabber grabber = FFmpegFrameGrabber.createDefault(filePath);
        return getBufferedImageByFrame(frameNum, grabber);
    }

    private static BufferedImage getBufferedImageByFrame(int frameNum, FFmpegFrameGrabber grabber)
            throws FrameGrabber.Exception {
        grabber.start();
        Frame frame;
        int i = 0;
        int fps = (int)grabber.getFrameRate();
        BufferedImage buffer = null;
        while (i < grabber.getLengthInFrames()) {
            frame = grabber.grabImage();
            if (i >= fps && i % (fps * frameNum) == 0) {
                Java2DFrameConverter converter = new Java2DFrameConverter();
                buffer = converter.getBufferedImage(frame);
                break;
            }
            i++;
        }
        grabber.stop();
        return buffer;
    }

    public static void main(String[] args) throws IOException {
        String filePath = "/Users/yangxuan/Desktop/1.mp4";
        BufferedImage buffer = getBufferedImageByFrame(1, filePath);
        saveImage(buffer, "asd.jpg", "jpg");
    }

    private static void saveImage(BufferedImage buffer, String fileName, String format) throws IOException {
        ImageIO.write(buffer, format, new File("/Users/yangxuan/Desktop/" + fileName));
    }
}
