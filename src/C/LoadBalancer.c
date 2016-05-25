#include <stdio.h>
#define EXIT 0

/**
 * Function to output a message
 */
void message(char *msg){
  printf("%s\n",msg);
}

int main() {
  message("Load Balancer");
  return 0;
}
