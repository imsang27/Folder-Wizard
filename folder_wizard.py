import os
import shutil
from typing import List, Tuple

class FolderWizard:
    def __init__(self):
        self.current_path = ""
        
    def main_menu(self) -> None:
        while True:
            print("\n=== 폴더 마법사 ===")
            print("1. 폴더 구조 상향 이동")
            print("2. 폴더 구조 하향 이동")
            print("3. 종료")
            
            choice = input("\n선택하세요: ")
            
            if choice == "1":
                self.move_up_structure()
            elif choice == "2":
                self.move_down_structure()
            elif choice == "3":
                print("프로그램을 종료합니다.")
                break
            else:
                print("잘못된 선택입니다. 다시 선택해주세요.")

    def get_path_input(self) -> str:
        path = input("대상 경로를 입력하세요: ").strip()
        if not os.path.exists(path):
            raise ValueError("존재하지 않는 경로입니다.")
        return path

    def move_up_structure(self) -> None:
        try:
            path = self.get_path_input()
            levels = int(input("상위로 이동할 레벨을 입력하세요: "))
            delete_empty = input("빈 폴더를 삭제할까요? (y/n): ").lower() == 'y'
            
            # 상위 이동 로직 구현
            self.process_up_movement(path, levels, delete_empty)
            print("상향 이동이 완료되었습니다.")
            
        except Exception as e:
            print(f"오류 발생: {e}")

    def move_down_structure(self) -> None:
        try:
            path = self.get_path_input()
            delimiters = input("구분자들을 입력하세요 (쉼표로 구분): ").split(',')
            chars_to_remove = input("제거할 문자들을 입력하세요 (쉼표로 구분, 생략 가능): ").split(',')
            
            # 하위 이동 로직 구현
            self.process_down_movement(path, delimiters, chars_to_remove)
            print("하향 이동이 완료되었습니다.")
            
        except Exception as e:
            print(f"오류 발생: {e}")

    def process_up_movement(self, path: str, levels: int, delete_empty: bool) -> None:
        for root, dirs, files in os.walk(path, topdown=False):
            for file in files:
                current_path = os.path.join(root, file)
                target_path = os.path.join(os.path.dirname(root), file)
                
                # 지정된 레벨만큼 상위로 이동
                for _ in range(levels - 1):
                    target_path = os.path.join(os.path.dirname(os.path.dirname(target_path)), file)
                
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                shutil.move(current_path, target_path)

        if delete_empty:
            self.remove_empty_folders(path)

    def process_down_movement(self, path: str, delimiters: List[str], chars_to_remove: List[str]) -> None:
        for root, _, files in os.walk(path):
            for file in files:
                current_path = os.path.join(root, file)
                processed_name = self.remove_chars(file, chars_to_remove)
                
                parts = [processed_name]
                for delimiter in delimiters:
                    new_parts = []
                    for part in parts:
                        new_parts.extend(part.split(delimiter))
                    parts = new_parts
                
                new_path = root
                for part in parts[:-1]:
                    new_path = os.path.join(new_path, part)
                    os.makedirs(new_path, exist_ok=True)
                
                target_path = os.path.join(new_path, parts[-1])
                if current_path != target_path:
                    shutil.move(current_path, target_path)

    @staticmethod
    def remove_chars(filename: str, chars_to_remove: List[str]) -> str:
        for char in chars_to_remove:
            if char:  # 빈 문자열이 아닌 경우에만 처리
                filename = filename.replace(char.strip(), '')
        return filename

    @staticmethod
    def remove_empty_folders(path: str) -> None:
        for root, dirs, files in os.walk(path, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    if not os.listdir(dir_path):  # 폴더가 비어있는 경우
                        os.rmdir(dir_path)
                except OSError:
                    continue

if __name__ == "__main__":
    wizard = FolderWizard()
    wizard.main_menu() 