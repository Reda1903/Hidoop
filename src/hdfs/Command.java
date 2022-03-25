package hdfs;

import java.io.Serializable;
import formats.Format;

public class Command implements Serializable {
    private static final long serialVersionUID = 1L;
    private ECommandType commandType;
    private String fileName;
    private String fileContent = null;
    private Format.Type format = null;

    public Command(ECommandType commandType, String fileName) {
        this.commandType = commandType;
        this.fileName = fileName;
    }

    public Command(ECommandType commandType, String fileName, Format.Type format) {
        this.commandType = commandType;
        this.fileName = fileName;
        this.format = format;
    }

    public ECommandType getCommandType() {
        return commandType;
    }

    public void setCommandType(ECommandType commandType) {
        this.commandType = commandType;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public Format.Type getFormat() {
        return format;
    }

    public void setFormat(Format.Type format) {
        this.format = format;
    }

    public String getFileContent() {
        return fileContent;
    }

    public void setFileContent(String fileContent) {
        this.fileContent = fileContent;
    }
}
