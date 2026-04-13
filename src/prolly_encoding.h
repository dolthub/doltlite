
#ifndef PROLLY_ENCODING_H
#define PROLLY_ENCODING_H

#define PROLLY_GET_U16(p) \
  ((u16)(p)[0] | ((u16)(p)[1]<<8))
#define PROLLY_PUT_U16(p,v) do{ \
  (p)[0]=(u8)(v); (p)[1]=(u8)((v)>>8); \
}while(0)

#define PROLLY_GET_U32(p) \
  ((u32)(p)[0] | ((u32)(p)[1]<<8) | ((u32)(p)[2]<<16) | ((u32)(p)[3]<<24))
#define PROLLY_PUT_U32(p,v) do{ \
  (p)[0]=(u8)(v); (p)[1]=(u8)((v)>>8); \
  (p)[2]=(u8)((v)>>16); (p)[3]=(u8)((v)>>24); \
}while(0)

#endif
