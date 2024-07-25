use super::Triple;

impl Triple {
    pub fn encode_spo(&self) -> [u8; 48] {
        let mut data = [0u8; 48];
        data[0..16].copy_from_slice(&self.sub.0.to_le_bytes());
        data[16..32].copy_from_slice(&self.pred.0.to_le_bytes());
        data[32..48].copy_from_slice(&self.obj.0.to_le_bytes());
        data
    }

    pub fn encode_pos(&self) -> [u8; 48] {
        let mut data = [0u8; 48];
        data[0..16].copy_from_slice(&self.pred.0.to_le_bytes());
        data[16..32].copy_from_slice(&self.obj.0.to_le_bytes());
        data[32..48].copy_from_slice(&self.sub.0.to_le_bytes());
        data
    }

    pub fn encode_osp(&self) -> [u8; 48] {
        let mut data = [0u8; 48];
        data[0..16].copy_from_slice(&self.obj.0.to_le_bytes());
        data[16..32].copy_from_slice(&self.sub.0.to_le_bytes());
        data[32..48].copy_from_slice(&self.pred.0.to_le_bytes());
        data
    }
}
