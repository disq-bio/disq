package htsjdk.samtools;

import htsjdk.samtools.util.BlockCompressedFilePointerUtil;
import java.util.ArrayList;
import java.util.List;

// TODO: move these methods to BlockCompressedFilePointerUtil/Chunk/Bin/BAMIndexMetaData
public class VirtualShiftUtil {
  /**
   * Move a virtual file pointer by a given (non-virtual) offset.
   *
   * @param virtualFilePointer the original virtual file pointer
   * @param offset the offset in bytes
   * @return a new virtual file pointer shifted by the given offset
   */
  static long shift(long virtualFilePointer, long offset) {
    if (virtualFilePointer == -1) {
      return -1;
    }
    long blockAddress = BlockCompressedFilePointerUtil.getBlockAddress(virtualFilePointer);
    int blockOffset = BlockCompressedFilePointerUtil.getBlockOffset(virtualFilePointer);
    return BlockCompressedFilePointerUtil.makeFilePointer(blockAddress + offset, blockOffset);
  }

  static Chunk shift(Chunk chunk, long offset) {
    return new Chunk(shift(chunk.getChunkStart(), offset), shift(chunk.getChunkEnd(), offset));
  }

  static Bin shift(Bin bin, long offset) {
    if (bin == null) {
      return null;
    }
    List<Chunk> chunkList = new ArrayList<>();
    for (Chunk chunk : bin.getChunkList()) {
      chunkList.add(shift(chunk, offset));
    }
    Bin newBin = new Bin(bin.getReferenceSequence(), bin.getBinNumber());
    newBin.setChunkList(chunkList);
    newBin.setLastChunk(chunkList.get(chunkList.size() - 1));
    return newBin;
  }

  static BAMIndexMetaData shift(BAMIndexMetaData metaData, long offset) {
    List<Chunk> chunkList = new ArrayList<>();
    long firstOffset = metaData.getFirstOffset();
    if (firstOffset != -1) { // -1 is unset, see BAMIndexMetaData
      firstOffset = shift(firstOffset, offset);
    }
    long lastOffset = metaData.getLastOffset();
    if (lastOffset != 0) { // 0 is unset, see BAMIndexMetaData
      lastOffset = shift(lastOffset, offset);
    }
    chunkList.add(new Chunk(firstOffset, lastOffset));
    chunkList.add(new Chunk(metaData.getAlignedRecordCount(), metaData.getUnalignedRecordCount()));
    return new BAMIndexMetaData(chunkList);
  }
}
