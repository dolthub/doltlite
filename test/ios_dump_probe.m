#import <Foundation/Foundation.h>
#import <TargetConditionals.h>
#include <stdlib.h>
#define main dumpProbeMain
#include "amalgamation_dump_probe.c"
#undef main

static int runExportProbe(NSString *directory){
  NSError *error=nil;
  if( ![[NSFileManager defaultManager] createDirectoryAtPath:directory
      withIntermediateDirectories:YES attributes:nil error:&error] ) return 1;
  NSString *destination=[directory stringByAppendingPathComponent:@"export"];
  char *args[]={"export-probe",(char*)[destination fileSystemRepresentation]};
  int rc=dumpProbeMain(2,args);
  if( rc==0 ){
    sqlite3 *db=0;
    sqlite3_stmt *query=0;
    NSString *path=[destination stringByAppendingString:@".sqlite"];
    rc=sqlite3_open_v2([path fileSystemRepresentation],&db,SQLITE_OPEN_READONLY,0);
    if( rc==SQLITE_OK ) rc=sqlite3_prepare_v2(db,"SELECT v FROM t WHERE id=7",-1,&query,0);
    if( rc==SQLITE_OK ){
      if( sqlite3_step(query)!=SQLITE_ROW || strcmp((const char*)sqlite3_column_text(query,0),"working") ) rc=SQLITE_ERROR;
    }
    sqlite3_finalize(query);query=0;
    if( rc==SQLITE_OK ) rc=sqlite3_prepare_v2(db,"PRAGMA integrity_check",-1,&query,0);
    if( rc==SQLITE_OK ){
      if( sqlite3_step(query)!=SQLITE_ROW || strcmp((const char*)sqlite3_column_text(query,0),"ok") ) rc=SQLITE_CORRUPT;
    }
    sqlite3_finalize(query);sqlite3_close(db);
  }
  NSString *result=[NSString stringWithFormat:@"%d",rc];
  if( ![result writeToFile:[directory stringByAppendingPathComponent:@"result.txt"]
                atomically:YES encoding:NSUTF8StringEncoding error:&error] ){
    NSLog(@"Cannot write export test result: %@",error);
    return 1;
  }
  return rc;
}

#if TARGET_OS_IPHONE
#import <UIKit/UIKit.h>
@interface ExportProbeDelegate : NSObject <UIApplicationDelegate>
@end
@implementation ExportProbeDelegate
- (BOOL)application:(UIApplication*)application
    didFinishLaunchingWithOptions:(NSDictionary*)options {
  dispatch_async(dispatch_get_main_queue(), ^{
    NSString *directory=[NSTemporaryDirectory() stringByAppendingPathComponent:@"doltlite-export-probe"];
    exit(runExportProbe(directory));
  });
  return YES;
}
@end
int main(int argc, char **argv){
  @autoreleasepool {
    return UIApplicationMain(argc,argv,nil,NSStringFromClass([ExportProbeDelegate class]));
  }
}
#else
int main(int argc, char **argv){
  @autoreleasepool {
    if( argc!=2 ) return 2;
    return runExportProbe([NSString stringWithUTF8String:argv[1]]);
  }
}
#endif
