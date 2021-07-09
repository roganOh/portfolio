import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class RecurcivelySha256 {

    public List<String> fileSearch(String path) {
        final var dir = new File(path);
        final var fileList = dir.listFiles();
//        if(fileList == null) {
//            throw new Exception("wrong path");
//        }
        final var files = new ArrayList<String>();
        try {
            for (int i = 0; i < fileList.length; i++) {
                final var file = fileList[i];

                if (file.isFile()) {
                    if (!file.getName().equals(".DS_Store")) {
                        System.out.println("파일 : " + file.getName());
                        System.out.println(file.getCanonicalPath());
                        System.out.println(sha256(file.getCanonicalPath()));
                        files.add(file.getName());
                    }
                } else if (file.isDirectory()) {
                    fileSearch(file.getCanonicalPath());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return files;
    }

    private static String sha256(final String path) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(path.getBytes("UTF-8"));
            StringBuffer hexString = new StringBuffer();

            for (int i = 0; i < hash.length; i++) {
                String hex = Integer.toHexString(0xff & hash[i]);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }

            return hexString.toString();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }
}
